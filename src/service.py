"""Logique partagée par l'API REST et l'IPC : description lisible d'une définition, activation.

La description est ce que l'utilisateur LIT avant d'approuver : elle doit refléter fidèlement la définition
(déclencheur, chaque étape, canaux de notification), pas un résumé flatteur.
"""

from __future__ import annotations

from datetime import datetime

from .definition import (
    AGENT_WRITE_TOOLS,
    AgentStep,
    Definition,
    DraftEmailStep,
    EmailReceivedTrigger,
    ListEventsStep,
    ListRemindersStep,
    LlmSummarizeStep,
    ManualTrigger,
    NotifyStep,
    ScheduleTrigger,
    SearchEmailsStep,
    WorkflowFinishedTrigger,
    definition_hash,
)
from .models import Workflow
from .schedule import next_run_after

_DAY_NAMES = {"mon": "lundi", "tue": "mardi", "wed": "mercredi", "thu": "jeudi", "fri": "vendredi", "sat": "samedi", "sun": "dimanche"}
_RANGES = {"today": "aujourd'hui", "tomorrow": "demain", "next_7_days": "les 7 prochains jours", "all": "tous"}
_CHANNELS = {"app": "l'application", "whatsapp": "ton WhatsApp", "email": "ton email"}
INSTRUCTION_PREVIEW_CHARS = 300


def schedule_text(trigger) -> str:
    if isinstance(trigger, ManualTrigger):
        return "Se lance à la demande (bouton « Lancer maintenant »)"
    if isinstance(trigger, EmailReceivedTrigger):
        return (
            f"À chaque nouvel email correspondant à « {trigger.query} » (vérifié toutes les {trigger.every_minutes} min ; "
            "les emails déjà présents à l'activation ne comptent pas)"
        )
    if isinstance(trigger, WorkflowFinishedTrigger):
        when = {"success": "se termine avec succès", "failure": "échoue", "any": "se termine (succès ou échec)"}[trigger.on]
        return f"Quand le workflow {trigger.workflow_id} {when}"
    where = f"à {trigger.at} ({trigger.timezone})"
    if trigger.every == "day":
        return f"Tous les jours {where}"
    if trigger.every == "weekdays":
        return f"Chaque jour ouvré (lundi à vendredi) {where}"
    days = ", ".join(_DAY_NAMES[d] for d in trigger.days) or "lundi"
    return f"Chaque semaine ({days}) {where}"


_GUARDS = {
    "has_results": "« {step} » a donné des résultats",
    "no_results": "« {step} » n'a rien donné",
    "contains": "« {step} » contient « {value} »",
    "not_contains": "« {step} » ne contient pas « {value} »",
}
_TOOL_NAMES = {
    "search_documents": "chercher dans les documents", "read_document": "lire un document", "inspect_table": "analyser un tableau",
    "read_image": "lire une image", "search_emails": "chercher dans tes emails", "list_upcoming_events": "lire ton agenda",
    "search_drive_files": "chercher dans ton Drive", "read_drive_file": "lire un fichier Drive",
    "create_document": "créer un document", "run_python": "exécuter du code isolé (sans réseau)",
}


def _step_line(step) -> str:
    line = _step_text(step)
    if step.only_if is not None:
        guard = _GUARDS[step.only_if.op].format(step=step.only_if.step, value=step.only_if.value)
        line = f"{line} — seulement si {guard}"
    return line


def _step_text(step) -> str:
    tail = " (facultatif)" if step.optional else ""
    if isinstance(step, ListEventsStep):
        return f"Lit ton agenda : {_RANGES[step.range]}{tail}"
    if isinstance(step, SearchEmailsStep):
        return f"Cherche dans tes emails « {step.query} » ({step.max_results} au plus){tail}"
    if isinstance(step, ListRemindersStep):
        return f"Lit tes rappels : {_RANGES[step.range]}{tail}"
    if step.type == "list_pending_actions":
        return f"Liste ce qui attend ton approbation{tail}"
    if isinstance(step, LlmSummarizeStep):
        instruction = step.instruction.strip().replace("\n", " ")
        if len(instruction) > INSTRUCTION_PREVIEW_CHARS:
            instruction = instruction[:INSTRUCTION_PREVIEW_CHARS] + "…"
        return f"Résume ({', '.join(step.inputs)}), {step.max_words} mots au plus, avec la consigne : « {instruction} »{tail}"
    if isinstance(step, AgentStep):
        goal = step.goal.strip().replace("\n", " ")
        if len(goal) > INSTRUCTION_PREVIEW_CHARS:
            goal = goal[:INSTRUCTION_PREVIEW_CHARS] + "…"
        tools = ", ".join(_TOOL_NAMES[t] for t in step.tools)
        writes = [t for t in step.tools if t in AGENT_WRITE_TOOLS]
        scope = " dans le projet choisi" if step.project_id else ""
        if step.agent_ref is not None:
            scope = f" avec le rôle d'un agent de projet figé à sa version {step.agent_ref.version} (dans le projet de cet agent)"
        notice = " Peut écrire des documents dans ton dossier." if writes else ""
        return (
            f"DONNA travaille seule{scope} (outils : {tools} ; {step.max_rounds} tours au plus) sur : « {goal} »."
            f"{notice} Aucun envoi à un tiers{tail}"
        )
    if isinstance(step, DraftEmailStep):
        files = " (avec les fichiers produits)" if step.attach_files else ""
        return (
            f"Prépare un email à {step.to}, objet « {step.subject} », avec le résultat de « {step.source} »{files} : "
            f"il attend ton approbation à CHAQUE exécution, rien ne part sans toi{tail}"
        )
    if isinstance(step, NotifyStep):
        channels = ", ".join(_CHANNELS[c] for c in step.channels)
        files = " avec les fichiers produits (dans l'application)" if step.attach_files else ""
        return f"T'envoie à toi seul le résultat — {channels}{files} — sous le titre « {step.title} »{tail}"
    return f"{step.type}{tail}"


def describe(definition: Definition) -> list[str]:
    """Lignes lisibles : le déclencheur puis une ligne par étape."""
    return [schedule_text(definition.trigger), *(_step_line(step) for step in definition.steps)]


def activate_workflow(workflow: Workflow, definition: Definition, now: datetime) -> None:
    """Approbation : enregistre l'empreinte de ce qui est approuvé et planifie la prochaine exécution."""
    workflow.status = "active"
    workflow.approved_hash = definition_hash(definition)
    workflow.approved_at = now
    if isinstance(definition.trigger, ScheduleTrigger):
        workflow.next_run_at = next_run_after(definition.trigger, now)
    elif isinstance(definition.trigger, EmailReceivedTrigger):
        workflow.next_run_at = now  # premier relevé tout de suite : il fixe l'état de départ (les emails déjà là ne comptent pas)
        workflow.state = None
    else:
        workflow.next_run_at = None
