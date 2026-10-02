"""Logique partagée par l'API REST et l'IPC : description lisible d'une définition, activation.

La description est ce que l'utilisateur LIT avant d'approuver : elle doit refléter fidèlement la définition
(déclencheur, chaque étape, canaux de notification), pas un résumé flatteur.
"""

from __future__ import annotations

from datetime import datetime

from .definition import (
    Definition,
    ListEventsStep,
    ListRemindersStep,
    LlmSummarizeStep,
    ManualTrigger,
    NotifyStep,
    ScheduleTrigger,
    SearchEmailsStep,
    definition_hash,
)
from .models import Workflow
from .schedule import next_run_after

_DAY_NAMES = {"mon": "lundi", "tue": "mardi", "wed": "mercredi", "thu": "jeudi", "fri": "vendredi", "sat": "samedi", "sun": "dimanche"}
_RANGES = {"today": "aujourd'hui", "tomorrow": "demain", "next_7_days": "les 7 prochains jours", "all": "tous"}
_CHANNELS = {"app": "l'application", "whatsapp": "ton WhatsApp", "email": "ton email"}
INSTRUCTION_PREVIEW_CHARS = 300


def schedule_text(trigger: ManualTrigger | ScheduleTrigger) -> str:
    if isinstance(trigger, ManualTrigger):
        return "Se lance à la demande (bouton « Lancer maintenant »)"
    where = f"à {trigger.at} ({trigger.timezone})"
    if trigger.every == "day":
        return f"Tous les jours {where}"
    if trigger.every == "weekdays":
        return f"Chaque jour ouvré (lundi à vendredi) {where}"
    days = ", ".join(_DAY_NAMES[d] for d in trigger.days) or "lundi"
    return f"Chaque semaine ({days}) {where}"


def _step_line(step) -> str:
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
    if isinstance(step, NotifyStep):
        channels = ", ".join(_CHANNELS[c] for c in step.channels)
        return f"T'envoie à toi seul le résultat — {channels} — sous le titre « {step.title} »{tail}"
    return f"{step.type}{tail}"


def describe(definition: Definition) -> list[str]:
    """Lignes lisibles : le déclencheur puis une ligne par étape."""
    return [schedule_text(definition.trigger), *(_step_line(step) for step in definition.steps)]


def activate_workflow(workflow: Workflow, definition: Definition, now: datetime) -> None:
    """Approbation : enregistre l'empreinte de ce qui est approuvé et planifie la prochaine exécution."""
    workflow.status = "active"
    workflow.approved_hash = definition_hash(definition)
    workflow.approved_at = now
    workflow.next_run_at = next_run_after(definition.trigger, now) if isinstance(definition.trigger, ScheduleTrigger) else None
