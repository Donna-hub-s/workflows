"""Exécuteurs d'étapes. Chacun renvoie du TEXTE (la « sortie » de l'étape) ou lève StepError.

Ce plugin ne touche NI à Google, NI au modèle, NI à WhatsApp : tout passe par les actions IPC du chat
(chat.workflow_read / summarize / agent / notify), qui gardent leurs garde-fous. Principes :
- lecture seule, ou message à l'utilisateur LUI-MÊME (notify) — jamais d'envoi vers un tiers sans approbation ;
- le modèle intervient dans le résumé (SANS outils : une donnée lue ne peut jamais provoquer d'action) et dans
  l'étape « agent », où il appelle des outils — mais seulement ceux de la définition approuvée, sans aucun outil
  d'envoi, dans des plafonds de tours et de durée (voir chat/workflow_service.py::agent) ;
- une étape ne reçoit les sorties des précédentes que par identifiant, jamais par évaluation de texte.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import update
from xcore.sdk import get_logger

from .definition import (
    AgentStep,
    DraftEmailStep,
    Guard,
    ListEventsStep,
    ListPendingActionsStep,
    ListRemindersStep,
    LlmSummarizeStep,
    NotifyStep,
    SearchEmailsStep,
)
from .models import Workflow

logger = get_logger("workflows.steps")


class StepError(Exception):
    """Échec d'une étape — message présentable (journal du run, note « indisponible »)."""


@dataclass
class StepEnv:
    deps: Any  # engine.WorkflowDeps
    tenant_id: str
    user_id: str
    workflow_id: str
    workflow_name: str
    conversation_id: str | None
    run_id: str
    outputs: dict[str, str] = field(default_factory=dict)
    # Fichiers produits par les étapes « agent » de ce run (métadonnées de stockage), joints par notify(attach_files).
    files: list[dict] = field(default_factory=list)
    # Journal des outils appelés par chaque étape « agent » (affiché dans l'historique du run).
    traces: dict[str, list] = field(default_factory=dict)
    # Étapes dont la sortie ne contient « rien » (aucun email, agenda vide…) : sert aux conditions.
    empty: set[str] = field(default_factory=set)


async def _chat(env: StepEnv, action: str, payload: dict) -> dict:
    try:
        result = await env.deps.call_plugin("chat", action, payload)
    except Exception as exc:
        logger.warning("appel IPC %s échoué : %s", action, exc)
        raise StepError("le service de chat n'a pas répondu") from exc
    if result.get("status") != "ok":
        raise StepError(str(result.get("msg") or "échec"))
    return result


async def _read(env: StepEnv, kind: str, params: dict) -> str:
    result = await _chat(env, "chat.workflow_read", {"tenant_id": env.tenant_id, "user_id": env.user_id, "kind": kind, "params": params})
    return str(result.get("text") or "")


async def list_events(env: StepEnv, step: ListEventsStep) -> str:
    return await _read(env, "events", {"range": step.range, "max_results": step.max_results})


async def search_emails(env: StepEnv, step: SearchEmailsStep) -> str:
    return await _read(env, "emails", {"query": step.query, "max_results": step.max_results})


async def list_reminders(env: StepEnv, step: ListRemindersStep) -> str:
    return await _read(env, "reminders", {"range": step.range})


async def list_pending_actions(env: StepEnv, step: ListPendingActionsStep) -> str:
    return await _read(env, "pending_actions", {})


async def llm_summarize(env: StepEnv, step: LlmSummarizeStep) -> str:
    blocks = {ref: env.outputs.get(ref, "") for ref in step.inputs}
    result = await _chat(
        env, "chat.workflow_summarize",
        {"instruction": step.instruction, "blocks": blocks, "max_words": step.max_words},
    )
    return str(result.get("text") or "")


async def agent(env: StepEnv, step: AgentStep) -> str:
    payload: dict[str, Any] = {
        "tenant_id": env.tenant_id, "user_id": env.user_id, "workflow_id": env.workflow_id, "run_id": env.run_id,
        "goal": step.goal, "blocks": {ref: env.outputs.get(ref, "") for ref in step.inputs}, "tools": list(step.tools),
        "max_rounds": step.max_rounds, "max_words": step.max_words,
    }
    if step.project_id:  # clé absente plutôt que None : la validation IPC refuse un None pour un str
        payload["project_id"] = step.project_id
    result = await _chat(env, "chat.workflow_agent", payload)
    env.files.extend(result.get("files") or [])
    env.traces[step.id] = result.get("trace") or []
    return str(result.get("text") or "")


async def draft_email(env: StepEnv, step: DraftEmailStep) -> str:
    """Prépare l'email (action en attente) — il ne part QUE si l'utilisateur l'approuve. Destinataire et objet
    viennent de la définition approuvée, jamais d'une sortie d'étape."""
    body = env.outputs[step.source].strip()
    if not body:
        return "Rien à envoyer (résultat vide)."
    payload: dict[str, Any] = {
        "tenant_id": env.tenant_id, "user_id": env.user_id, "workflow_id": env.workflow_id,
        "workflow_name": env.workflow_name, "to": step.to, "subject": step.subject, "body": body,
    }
    if step.attach_files and env.files:
        payload["files"] = env.files
    if env.conversation_id:
        payload["conversation_id"] = env.conversation_id
    result = await _chat(env, "chat.workflow_draft_email", payload)
    conversation_id = result.get("conversation_id")
    if conversation_id and conversation_id != env.conversation_id:
        env.conversation_id = conversation_id
        async with env.deps.db.session() as session:
            await session.execute(update(Workflow).where(Workflow.id == env.workflow_id).values(conversation_id=conversation_id))
    return f"Email à {step.to} préparé : en attente de ton approbation (rien n'est parti)."


async def notify(env: StepEnv, step: NotifyStep) -> str:
    if not env.outputs[step.source].strip():
        return "Rien à envoyer (résultat vide)."
    payload = {
        "tenant_id": env.tenant_id, "user_id": env.user_id, "workflow_id": env.workflow_id,
        "workflow_name": env.workflow_name, "title": step.title, "text": env.outputs[step.source],
        "channels": list(step.channels),
    }
    if step.attach_files and env.files:
        payload["files"] = env.files
    if env.conversation_id:  # clé absente plutôt que None : la validation IPC refuse un None pour un str
        payload["conversation_id"] = env.conversation_id
    result = await _chat(env, "chat.workflow_notify", payload)
    conversation_id = result.get("conversation_id")
    if conversation_id and conversation_id != env.conversation_id:
        env.conversation_id = conversation_id
        async with env.deps.db.session() as session:
            await session.execute(update(Workflow).where(Workflow.id == env.workflow_id).values(conversation_id=conversation_id))
    delivered, problems = result.get("delivered") or [], result.get("problems") or []
    if not delivered:
        raise StepError("aucun canal n'a abouti : " + " ; ".join(problems))
    return f"Envoyé : {', '.join(delivered)}" + (f" — non envoyé : {', '.join(problems)}" if problems else "")


# Début des textes que les lectures renvoient quand il n'y a rien (voir chat/workflow_service.py et tools._search_emails).
_NOTHING = ("Aucun", "Rien en attente")
READ_STEP_TYPES = frozenset({"list_events", "search_emails", "list_reminders", "list_pending_actions"})


def is_empty(step_type: str, text: str) -> bool:
    """Vrai si l'étape n'a rien produit d'utile : sortie vide, ou — pour une lecture — son message « aucun résultat »."""
    stripped = text.strip()
    if not stripped:
        return True
    return step_type in READ_STEP_TYPES and stripped.startswith(_NOTHING)


def guard_allows(env: StepEnv, guard: Guard) -> bool:
    """Évalue la condition d'une étape sur la sortie de l'étape visée (texte seulement, aucune évaluation)."""
    text = env.outputs.get(guard.step, "")
    if guard.op == "has_results":
        return guard.step not in env.empty
    if guard.op == "no_results":
        return guard.step in env.empty
    needle = (guard.value or "").lower()
    found = needle in text.lower()
    return found if guard.op == "contains" else not found


EXECUTORS = {
    "list_events": list_events,
    "search_emails": search_emails,
    "list_reminders": list_reminders,
    "list_pending_actions": list_pending_actions,
    "llm_summarize": llm_summarize,
    "agent": agent,
    "draft_email": draft_email,
    "notify": notify,
}
