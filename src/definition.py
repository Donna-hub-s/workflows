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

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, field_validator, model_validator

MAX_STEPS = 20
MAX_INSTRUCTION_CHARS = 2000
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


Trigger = Annotated[Union[ManualTrigger, ScheduleTrigger], Field(discriminator="type")]


# ── Étapes ────────────────────────────────────────────────────────────────────


class _Step(_Strict):
    id: str
    # Étape facultative : si elle échoue (ou si sa compétence est coupée), le workflow continue avec une
    # note « indisponible » au lieu de s'arrêter.
    optional: bool = False

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


class NotifyStep(_Step):
    """Message à l'UTILISATEUR LUI-MÊME uniquement (application, son numéro WhatsApp, son email) — jamais
    un tiers : c'est ce qui permet de l'exécuter sans approbation à chaque fois."""

    type: Literal["notify"]
    source: str
    title: str = Field(min_length=1, max_length=80)
    channels: list[Literal["app", "whatsapp", "email"]] = Field(min_length=1, max_length=3)

    @field_validator("channels")
    @classmethod
    def _unique(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))


Step = Annotated[
    Union[ListEventsStep, SearchEmailsStep, ListRemindersStep, ListPendingActionsStep, LlmSummarizeStep, NotifyStep],
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
    "notify": "write",
}


class Definition(_Strict):
    trigger: Trigger
    steps: list[Step] = Field(min_length=1, max_length=MAX_STEPS)

    @model_validator(mode="after")
    def _references(self) -> "Definition":
        seen: set[str] = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"Identifiant d'étape en double : {step.id!r}.")
            refs: list[str] = []
            if isinstance(step, LlmSummarizeStep):
                refs = step.inputs
            elif isinstance(step, NotifyStep):
                refs = [step.source]
            for ref in refs:
                if ref not in seen:
                    raise ValueError(f"L'étape {step.id!r} utilise {ref!r}, qui n'existe pas ou vient plus tard.")
            seen.add(step.id)
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
            problems.append(f"{_where(error['loc'])} : {error['msg'].removeprefix('Value error, ')}")
        raise DefinitionError("Définition invalide — " + " ; ".join(problems)) from exc


def canonical_json(definition: Definition) -> str:
    return json.dumps(definition.model_dump(mode="json"), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def definition_hash(definition: Definition) -> str:
    """Empreinte de la forme canonique : l'ordre des clés ou les espaces ne changent pas ce qui a été approuvé."""
    return hashlib.sha256(canonical_json(definition).encode("utf-8")).hexdigest()
