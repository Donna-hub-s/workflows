"""Définition d'un workflow : un déclencheur et une liste d'étapes, validés AVANT toute exécution.

Format JSON, versionné par son empreinte (definition_hash) : ce que l'utilisateur approuve est exactement
ce qui s'exécute, et toute modification réclame une nouvelle approbation (voir routes/workflow_routes.py).

    {"trigger": {"type": "schedule", "every": "weekdays", "at": "08:00", "timezone": "Africa/Ouagadougou"},
     "steps": [
       {"id": "agenda", "type": "list_events", "range": "today", "optional": true},
       {"id": "resume", "type": "llm_summarize", "inputs": ["agenda"], "instruction": "Fais un briefing."},
       {"id": "envoi", "type": "notify", "source": "resume", "title": "Briefing", "channels": ["app"]}]}

Aucune étape n'exécute de code ni n'appelle d'outil choisi par le modèle : les types sont une liste
blanche fermée, et les valeurs passent d'une étape à l'autre par identifiant (`inputs`, `source`).
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Literal, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, EmailStr, Field, TypeAdapter, ValidationError, field_validator, model_validator

MAX_STEPS = 20
MAX_INSTRUCTION_CHARS = 2000
MAX_AGENT_ROUNDS = 12
MAX_DRAFT_EMAIL_STEPS = 3
TRIGGER_REF = "trigger"  # identifiant réservé : ce qui a déclenché un run (événement) comme sortie d'étape

# Outils que l'étape « agent » peut recevoir : liste FERMÉE, fixée dans la définition que l'utilisateur approuve.
# Aucun outil d'envoi ni d'approbation (propose_*, confirm_action…) : ce que l'agent produit ne sort de
# l'application que par une étape « notify » (vers l'utilisateur lui-même) ou une action en attente.
# Les noms sont ceux des outils du chat (app/chat/src/tools.py).
AGENT_READ_TOOLS = (
    "search_documents", "read_document", "inspect_table", "read_image",
    "search_emails", "list_upcoming_events", "search_drive_files", "read_drive_file",
)
AGENT_WRITE_TOOLS = ("create_document", "run_python")  # écrivent dans le dossier de documents de l'utilisateur
AGENT_TOOLS = AGENT_READ_TOOLS + AGENT_WRITE_TOOLS
AgentTool = Literal[
    "search_documents", "read_document", "inspect_table", "read_image",
    "search_emails", "list_upcoming_events", "search_drive_files", "read_drive_file",
    "create_document", "run_python",
]
# Motifs en chaîne + re.match (et non re.compile) : l'analyse statique du plugin signale l'attribut `compile`.
STEP_ID_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"
TIME_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

Weekday = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class DefinitionError(ValueError):
    """Définition refusée — message présentable à l'utilisateur (et au modèle qui l'a proposée)."""


class _Strict(BaseModel):
    # Une clé inconnue est une erreur : jamais d'option « silencieusement ignorée » dans ce qui a été approuvé.
    model_config = ConfigDict(extra="forbid")


# ── Déclencheurs ──────────────────────────────────────────────────────────────


class ManualTrigger(_Strict):
    type: Literal["manual"]


class ScheduleTrigger(_Strict):
    type: Literal["schedule"]
    every: Literal["day", "weekdays", "week"]
    at: str
    days: list[Weekday] = Field(default_factory=list)  # seulement pour « week » ; vide = lundi
    timezone: str = "UTC"

    @field_validator("at")
    @classmethod
    def _time(cls, value: str) -> str:
        if not re.match(TIME_PATTERN, value):
            raise ValueError("`at` doit être une heure HH:MM (00:00 à 23:59).")
        return value

    @field_validator("timezone")
    @classmethod
    def _zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError, OSError):
            raise ValueError(f"Fuseau horaire inconnu : {value!r} (nom IANA attendu, ex. 'Europe/Paris').")
        return value

    @model_validator(mode="after")
    def _days(self) -> "ScheduleTrigger":
        if self.every != "week" and self.days:
            raise ValueError("`days` n'est utilisable qu'avec every='week'.")
        self.days = sorted(set(self.days), key=WEEKDAYS.index)
        return self


class EmailReceivedTrigger(_Strict):
    """Un nouvel email correspond à une recherche Gmail. Vérifié toutes les `every_minutes` minutes ; les emails déjà
    présents à l'activation ne déclenchent rien (seuls les nouveaux comptent)."""

    type: Literal["email_received"]
    query: str = Field(min_length=1, max_length=200)  # syntaxe de recherche Gmail
    every_minutes: int = Field(10, ge=5, le=60)


class WorkflowFinishedTrigger(_Strict):
    """Un autre workflow de l'utilisateur vient de se terminer — pour les enchaîner. Les enchaînements sont limités en
    profondeur (le plugin refuse les boucles)."""

    type: Literal["workflow_finished"]
    workflow_id: str = Field(min_length=1, max_length=36)
    on: Literal["success", "failure", "any"] = "success"


EVENT_TRIGGER_TYPES = ("email_received", "workflow_finished")

Trigger = Annotated[
    Union[ManualTrigger, ScheduleTrigger, EmailReceivedTrigger, WorkflowFinishedTrigger], Field(discriminator="type")
]


# ── Étapes ────────────────────────────────────────────────────────────────────


class Guard(_Strict):
    """Condition d'exécution d'une étape (« seulement si… ») sur la sortie d'une étape précédente.
    Comparaison de texte uniquement : aucune expression évaluée, rien d'exécutable."""

    step: str
    op: Literal["has_results", "no_results", "contains", "not_contains"]
    value: str | None = Field(default=None, min_length=1, max_length=200)  # requis pour contains / not_contains

    @model_validator(mode="after")
    def _value(self) -> "Guard":
        needs = self.op in ("contains", "not_contains")
        if needs and not self.value:
            raise ValueError(f"la condition « {self.op} » demande une valeur à chercher.")
        if not needs and self.value is not None:
            raise ValueError(f"la condition « {self.op} » n'a pas de valeur.")
        return self


class _Step(_Strict):
    id: str
    # Étape facultative : si elle échoue (ou si sa compétence est coupée), le workflow continue avec une
    # note « indisponible » au lieu de s'arrêter.
    optional: bool = False
    # Condition : l'étape est ignorée (et son résultat reste vide) si elle n'est pas remplie.
    only_if: Guard | None = None

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        if not re.match(STEP_ID_PATTERN, value):
            raise ValueError(f"Identifiant d'étape invalide : {value!r} (minuscules, chiffres et _, 32 caractères max).")
        return value


class ListEventsStep(_Step):
    type: Literal["list_events"]
    range: Literal["today", "tomorrow", "next_7_days"] = "today"
    max_results: int = Field(10, ge=1, le=20)


class SearchEmailsStep(_Step):
    type: Literal["search_emails"]
    query: str = Field(min_length=1, max_length=200)  # syntaxe de recherche Gmail
    max_results: int = Field(10, ge=1, le=10)


class ListRemindersStep(_Step):
    type: Literal["list_reminders"]
    range: Literal["today", "next_7_days", "all"] = "today"


class ListPendingActionsStep(_Step):
    type: Literal["list_pending_actions"]


class LlmSummarizeStep(_Step):
    type: Literal["llm_summarize"]
    inputs: list[str] = Field(min_length=1, max_length=10)
    instruction: str = Field(min_length=1, max_length=MAX_INSTRUCTION_CHARS)
    max_words: int = Field(150, ge=20, le=500)


class AgentRef(_Strict):
    """Un agent de projet (voir app/chat/src/agents.py) figé À UNE VERSION. Les versions d'un agent ne changent jamais : tant que
    l'utilisateur n'a pas approuvé une autre version, c'est exactement ce texte de consignes qui s'applique — qu'un membre du
    projet modifie l'agent entre-temps ne change rien au comportement de ce workflow."""

    id: str = Field(min_length=1, max_length=36)
    version: int = Field(ge=1, le=1_000_000)


class AgentStep(_Step):
    """Étape « agentique » : DONNA travaille par elle-même avec la boucle d'outils du chat (lire, chercher, analyser,
    calculer, rédiger), dans les limites de la définition approuvée : ces outils-là seulement, ce nombre de tours
    au plus, ce délai. Elle décide quoi lire et quand s'arrêter ; elle ne peut ni ajouter d'outil, ni envoyer
    quoi que ce soit. Les données lues (documents, emails) restent de la donnée, jamais des instructions."""

    type: Literal["agent"]
    goal: str = Field(min_length=1, max_length=MAX_INSTRUCTION_CHARS)
    tools: list[AgentTool] = Field(min_length=1, max_length=len(AGENT_TOOLS))
    inputs: list[str] = Field(default_factory=list, max_length=10)  # sorties d'étapes précédentes (données)
    project_id: str | None = Field(default=None, min_length=1, max_length=36)  # périmètre documentaire
    max_rounds: int = Field(8, ge=1, le=MAX_AGENT_ROUNDS)
    max_words: int = Field(300, ge=20, le=800)
    # Rôle d'un agent de projet, à une version précise. Il ne fait que RESTREINDRE : outils de l'étape ∩ outils de l'agent, et ses
    # consignes passent sous les règles de DONNA. None (et non {}) quand absent : l'empreinte des définitions déjà approuvées
    # ne doit pas changer.
    agent_ref: AgentRef | None = None

    @field_validator("tools")
    @classmethod
    def _unique_tools(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def _scope(self) -> "AgentStep":
        if self.agent_ref is not None and self.project_id is not None:
            raise ValueError("avec un agent de projet, le périmètre documentaire est le projet de l'agent : ne donne pas `project_id`.")
        return self


class DraftEmailStep(_Step):
    """Prépare un email à un TIERS — jamais ne l'envoie : il apparaît dans les actions à approuver, et l'utilisateur
    l'approuve (ou non) À CHAQUE exécution, après avoir lu le texte. Le destinataire et l'objet sont fixés ici, dans la
    définition que l'utilisateur approuve ; seul le corps vient d'une étape précédente."""

    type: Literal["draft_email"]
    to: EmailStr
    subject: str = Field(min_length=1, max_length=200)
    source: str  # étape dont la sortie devient le corps du message
    attach_files: bool | None = None  # joint les fichiers produits par les étapes « agent » de ce run

    @field_validator("subject")
    @classmethod
    def _one_line(cls, value: str) -> str:
        if "\n" in value or "\r" in value:
            raise ValueError("l'objet tient sur une seule ligne.")
        return value

    @field_validator("attach_files")
    @classmethod
    def _attach(cls, value: bool | None) -> bool | None:
        return True if value else None


class NotifyStep(_Step):
    """Message à l'UTILISATEUR LUI-MÊME uniquement (application, son numéro WhatsApp, son email) — jamais
    un tiers : c'est ce qui permet de l'exécuter sans approbation à chaque fois."""

    type: Literal["notify"]
    source: str
    title: str = Field(min_length=1, max_length=80)
    channels: list[Literal["app", "whatsapp", "email"]] = Field(min_length=1, max_length=3)
    # Joint à la notification de l'application les fichiers produits par les étapes « agent » de ce run.
    # None (et non False) quand absent : l'empreinte des notify déjà approuvés ne doit pas changer.
    attach_files: bool | None = None

    @field_validator("attach_files")
    @classmethod
    def _attach(cls, value: bool | None) -> bool | None:
        return True if value else None

    @field_validator("channels")
    @classmethod
    def _unique(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))


Step = Annotated[
    Union[
        ListEventsStep, SearchEmailsStep, ListRemindersStep, ListPendingActionsStep, LlmSummarizeStep, AgentStep,
        DraftEmailStep, NotifyStep,
    ],
    Field(discriminator="type"),
]

# Niveau de risque d'un type d'étape (mêmes valeurs que skills.RISKS). Aucune étape « outbound » en v1 :
# envoyer à un tiers passera par une action en attente que l'utilisateur approuve à chaque exécution.
STEP_RISK: dict[str, str] = {
    "list_events": "read",
    "search_emails": "read",
    "list_reminders": "read",
    "list_pending_actions": "read",
    "llm_summarize": "read",
    "agent": "write",  # peut écrire des documents (create_document, run_python) dans le dossier de l'utilisateur
    "draft_email": "outbound",  # un tiers est destinataire — mais seulement après approbation humaine à chaque run
    "notify": "write",
}


class Definition(_Strict):
    trigger: Trigger
    steps: list[Step] = Field(min_length=1, max_length=MAX_STEPS)

    @model_validator(mode="after")
    def _references(self) -> "Definition":
        # Un déclencheur par événement fournit « trigger » (ce qui l'a déclenché) comme une sortie d'étape.
        is_event = self.trigger.type in EVENT_TRIGGER_TYPES
        seen: set[str] = {TRIGGER_REF} if is_event else set()
        for step in self.steps:
            if step.id == TRIGGER_REF:
                raise ValueError(f"L'identifiant {TRIGGER_REF!r} est réservé.")
            if step.id in seen:
                raise ValueError(f"Identifiant d'étape en double : {step.id!r}.")
            refs: list[str] = []
            if isinstance(step, (LlmSummarizeStep, AgentStep)):
                refs = list(step.inputs)
            elif isinstance(step, (NotifyStep, DraftEmailStep)):
                refs = [step.source]
            if step.only_if is not None:
                refs.append(step.only_if.step)
            for ref in refs:
                if ref not in seen:
                    hint = f" (« {TRIGGER_REF} » n'existe que pour un déclencheur par événement)" if ref == TRIGGER_REF else ""
                    raise ValueError(f"L'étape {step.id!r} utilise {ref!r}, qui n'existe pas ou vient plus tard{hint}.")
            seen.add(step.id)
        if sum(isinstance(step, DraftEmailStep) for step in self.steps) > MAX_DRAFT_EMAIL_STEPS:
            raise ValueError(f"{MAX_DRAFT_EMAIL_STEPS} emails à approuver par exécution au plus.")
        return self


_ADAPTER = TypeAdapter(Definition)


def _where(loc: tuple) -> str:
    """Chemin d'une erreur de validation, lisible : « étape 2 › list_events › max_results »."""
    parts: list[str] = []
    for part in loc:
        if part == "steps":
            continue
        if part == "trigger":
            parts.append("déclencheur")
        elif isinstance(part, int):
            parts.append(f"étape {part + 1}")
        else:
            parts.append(str(part))
    return " › ".join(parts) or "définition"


DEFAULT_MAX_RUNS_PER_DAY = 24
# Une exécution d'agent consomme beaucoup de jetons (plusieurs appels au modèle, jusqu'à 12 tours) : le plafond par
# défaut est plus bas que pour un briefing. L'utilisateur peut le relever (jusqu'à 96) dans les réglages du workflow.
AGENT_DEFAULT_MAX_RUNS_PER_DAY = 6


def default_max_runs(definition: Definition) -> int:
    """Plafond d'exécutions par jour d'un nouveau workflow, selon ce qu'il fait."""
    uses_agent = any(isinstance(step, AgentStep) for step in definition.steps)
    return AGENT_DEFAULT_MAX_RUNS_PER_DAY if uses_agent else DEFAULT_MAX_RUNS_PER_DAY


def parse_definition(raw: dict | str) -> Definition:
    """Valide une définition (dict ou JSON). Lève DefinitionError avec un message lisible."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return _ADAPTER.validate_python(data)
    except json.JSONDecodeError as exc:
        raise DefinitionError(f"Définition illisible (JSON invalide) : {exc.msg}.") from exc
    except ValidationError as exc:
        problems = []
        for error in exc.errors()[:5]:
            message = error["msg"].removeprefix("Value error, ")
            if error["type"] == "union_tag_invalid":
                # Message pydantic technique -> consigne lisible (aussi pour le modèle qui a proposé la définition).
                allowed = ", ".join(sorted(str(tag).strip("'") for tag in error["ctx"].get("expected_tags", "").split(", ")))
                message = f"type {error['ctx'].get('tag')!r} inconnu — types autorisés : {allowed}"
            problems.append(f"{_where(error['loc'])} : {message}")
        raise DefinitionError("Définition invalide — " + " ; ".join(problems)) from exc


def canonical_json(definition: Definition) -> str:
    # exclude_none : un champ facultatif ajouté plus tard (valeur None) ne change pas l'empreinte des définitions
    # déjà approuvées — sinon chaque évolution du format désapprouverait les workflows existants.
    return json.dumps(definition.model_dump(mode="json", exclude_none=True), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def definition_hash(definition: Definition) -> str:
    """Empreinte de la forme canonique : l'ordre des clés ou les espaces ne changent pas ce qui a été approuvé."""
    return hashlib.sha256(canonical_json(definition).encode("utf-8")).hexdigest()
