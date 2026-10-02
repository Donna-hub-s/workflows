"""Exécuteurs d'étapes. Chacun renvoie du TEXTE (la « sortie » de l'étape) ou lève StepError.

Ce plugin ne touche NI à Google, NI au modèle, NI à WhatsApp : tout passe par les actions IPC du chat
(chat.workflow_read / summarize / notify), qui gardent leurs garde-fous. Principes :
- lecture seule, ou message à l'utilisateur LUI-MÊME (notify) — jamais d'envoi vers un tiers en v1 ;
- le modèle n'intervient que dans le résumé, SANS outils : une donnée lue (email, agenda) ne peut donc
  jamais provoquer d'action, seulement colorer un résumé ;
- une étape ne reçoit les sorties des précédentes que par identifiant, jamais par évaluation de texte.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import update
from xcore.sdk import get_logger

from .definition import (
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


async def notify(env: StepEnv, step: NotifyStep) -> str:
    payload = {
        "tenant_id": env.tenant_id, "user_id": env.user_id, "workflow_id": env.workflow_id,
        "workflow_name": env.workflow_name, "title": step.title, "text": env.outputs[step.source],
        "channels": list(step.channels),
    }
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


EXECUTORS = {
    "list_events": list_events,
    "search_emails": search_emails,
    "list_reminders": list_reminders,
    "list_pending_actions": list_pending_actions,
    "llm_summarize": llm_summarize,
    "notify": notify,
}
