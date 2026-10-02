"""Exécution d'un run de workflow : journal, plafonds, délais.

Deux temps, pour que l'appelant (route « lancer maintenant », planificateur) puisse répondre avec l'id du run
avant la fin de l'exécution : create_run (enregistrement, vérifications) puis execute_run (étapes).

Garde-fous : un run planifié ne part que si le workflow est actif ET que sa définition a encore l'empreinte
approuvée ; plafonds d'exécutions par jour (workflow et utilisateur) ; délai par étape et par run ; aucune
transaction n'est gardée ouverte pendant un appel IPC (SQLite est partagé avec l'API). Les compétences coupées
par l'utilisateur sont appliquées par le chat, qui refuse alors la lecture (l'étape échoue ou est ignorée
si elle est facultative).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, update
from xcore.sdk import get_logger

from .definition import EVENT_TRIGGER_TYPES, TRIGGER_REF, AgentStep, DefinitionError, WorkflowFinishedTrigger, definition_hash, parse_definition
from .models import Workflow, WorkflowRun, WorkflowStepRun
from .steps import EXECUTORS, StepEnv, StepError, guard_allows, is_empty

logger = get_logger("workflows.engine")

STEP_TIMEOUT_S = 120
RUN_TIMEOUT_S = 300
# Une étape « agent » enchaîne plusieurs appels au modèle et à des outils : plus de temps, et le délai du run
# s'allonge d'autant par étape agent (jamais au-delà de MAX_RUN_TIMEOUT_S).
AGENT_STEP_TIMEOUT_S = 600
MAX_RUN_TIMEOUT_S = 3600
USER_RUNS_PER_DAY = 200
STORED_OUTPUT_CHARS = 4000
STORED_DETAIL_CHARS = 20000
MAX_CONCURRENT_RUNS = 3
# Enchaînement « workflow terminé → workflow suivant » : au-delà, on s'arrête (A→B→A ne boucle pas).
MAX_CHAIN_DEPTH = 3
FOLLOWER_CONTEXT_CHARS = 3000

_slots: asyncio.Semaphore | None = None
_background: set[asyncio.Task] = set()  # runs lancés par un enchaînement (on garde une référence : sinon ramassés)


def spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task


def _semaphore() -> asyncio.Semaphore:
    global _slots
    if _slots is None:
        _slots = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    return _slots


@dataclass
class WorkflowDeps:
    """Services du plugin : la base et le pont IPC (tout le reste passe par le plugin chat)."""

    db: Any
    call_plugin: Any = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def create_run(
    deps: WorkflowDeps, workflow_id: str, trigger: str, context: str | None = None, depth: int = 0
) -> str | None:
    """Enregistre un run et renvoie son id, ou None si le workflow n'a pas le droit de tourner.

    `context` : ce qui a déclenché un run par événement (emails reçus…), disponible aux étapes comme « trigger »."""
    now = _now()
    async with deps.db.session() as session:
        workflow = await session.get(Workflow, workflow_id)
        if workflow is None or workflow.status == "archived":
            return None
        try:
            definition = parse_definition(workflow.definition)
            digest = definition_hash(definition)
            problem = None
        except DefinitionError as exc:
            digest, problem = "", str(exc)

        if trigger in ("schedule", "event") and (workflow.status != "active" or workflow.approved_hash != digest):
            # Défense en profondeur : seule une définition approuvée tourne seule.
            logger.warning("run planifié refusé (non actif ou définition modifiée)", workflow=workflow_id)
            return None

        run = WorkflowRun(
            workflow_id=workflow.id,
            tenant_id=workflow.tenant_id,
            user_id=workflow.user_id,
            trigger=trigger,
            trigger_context=(context or None) and context[:FOLLOWER_CONTEXT_CHARS],
            depth=depth,
            definition_hash=digest,
            started_at=now,
        )
        if problem is not None:
            run.status, run.error, run.finished_at = "failed", problem, now
        else:
            since = now - timedelta(days=1)
            counted = WorkflowRun.status != "skipped"
            per_workflow = (
                await session.execute(
                    select(func.count()).select_from(WorkflowRun).where(
                        WorkflowRun.workflow_id == workflow.id, WorkflowRun.started_at >= since, counted
                    )
                )
            ).scalar_one()
            per_user = (
                await session.execute(
                    select(func.count()).select_from(WorkflowRun).where(
                        WorkflowRun.user_id == workflow.user_id, WorkflowRun.started_at >= since, counted
                    )
                )
            ).scalar_one()
            if per_workflow >= workflow.max_runs_per_day or per_user >= USER_RUNS_PER_DAY:
                run.status, run.finished_at = "skipped", now
                run.error = "Plafond d'exécutions par jour atteint."
        session.add(run)
        await session.flush()
        return run.id


async def _record_step(
    deps: WorkflowDeps, run_id: str, position: int, step: Any, status: str, started: datetime,
    output: str | None = None, error: str | None = None, detail: list | None = None,
) -> None:
    async with deps.db.session() as session:
        session.add(
            WorkflowStepRun(
                run_id=run_id,
                position=position,
                step_id=step.id,
                type=step.type,
                status=status,
                output=(output or "")[:STORED_OUTPUT_CHARS] or None,
                error=error,
                detail=json.dumps(detail, ensure_ascii=False)[:STORED_DETAIL_CHARS] if detail else None,
                started_at=started,
                finished_at=_now(),
            )
        )


async def _finish(
    deps: WorkflowDeps, run_id: str, workflow_id: str, status: str, error: str | None, result: str | None = None,
    chain: bool = True,
) -> None:
    now = _now()
    async with deps.db.session() as session:
        await session.execute(
            update(WorkflowRun).where(WorkflowRun.id == run_id).values(status=status, error=error, finished_at=now)
        )
        await session.execute(update(Workflow).where(Workflow.id == workflow_id).values(last_run_at=now))
    if not chain:
        return
    try:
        await trigger_followers(deps, run_id, status, error, result)
    except Exception:
        logger.exception("déclenchement des workflows suivants en échec", run=run_id)


async def trigger_followers(deps: WorkflowDeps, run_id: str, status: str, error: str | None, result: str | None) -> list[str]:
    """Lance les workflows ACTIFS du même utilisateur qui attendent la fin de celui-ci (déclencheur `workflow_finished`).

    Garde-fous : même utilisateur seulement, définition approuvée (create_run), profondeur d'enchaînement bornée
    (A→B→A s'arrête), plafonds d'exécutions par jour habituels."""
    async with deps.db.session() as session:
        run = await session.get(WorkflowRun, run_id)
        if run is None or run.depth >= MAX_CHAIN_DEPTH:
            return []
        finished = await session.get(Workflow, run.workflow_id)
        candidates = (
            await session.execute(
                select(Workflow.id, Workflow.definition).where(
                    Workflow.user_id == run.user_id, Workflow.tenant_id == run.tenant_id,
                    Workflow.status == "active", Workflow.id != run.workflow_id,
                )
            )
        ).all()
        depth, name = run.depth + 1, finished.name if finished else "?"
    started: list[str] = []
    ok = status == "succeeded"
    for workflow_id, definition_json in candidates:
        try:
            trigger = parse_definition(definition_json).trigger
        except DefinitionError:
            continue
        if not isinstance(trigger, WorkflowFinishedTrigger) or trigger.workflow_id != run.workflow_id:
            continue
        if (trigger.on == "success" and not ok) or (trigger.on == "failure" and ok):
            continue
        outcome = "réussi" if ok else f"en échec ({error})"
        context = f"Le workflow « {name} » vient de se terminer : {outcome}." + (f"\nRésultat :\n{result}" if result else "")
        follower = await create_run(deps, workflow_id, "event", context=context, depth=depth)
        if follower:
            started.append(follower)
            spawn(execute_run(deps, follower))
    return started


def run_timeout(definition_json: str) -> int:
    """Délai du run : la base, plus le temps d'agent de chaque étape « agent »."""
    try:
        agents = sum(1 for step in parse_definition(definition_json).steps if isinstance(step, AgentStep))
    except DefinitionError:
        agents = 0
    return min(RUN_TIMEOUT_S + AGENT_STEP_TIMEOUT_S * agents, MAX_RUN_TIMEOUT_S)


async def execute_run(deps: WorkflowDeps, run_id: str) -> None:
    """Exécute les étapes d'un run créé par create_run. Ne lève jamais : tout est consigné dans le run."""
    async with _semaphore():
        async with deps.db.session() as session:
            run = await session.get(WorkflowRun, run_id)
            if run is None or run.status != "running":
                return
            workflow = await session.get(Workflow, run.workflow_id)
            env = StepEnv(
                deps=deps, tenant_id=workflow.tenant_id, user_id=workflow.user_id, workflow_id=workflow.id,
                workflow_name=workflow.name, conversation_id=workflow.conversation_id, run_id=run_id,
            )
            if run.trigger == "event":  # ce qui a déclenché le run, lisible par les étapes (donnée, jamais une consigne)
                env.outputs[TRIGGER_REF] = run.trigger_context or ""
            definition_json = workflow.definition
        timeout = run_timeout(definition_json)
        try:
            await asyncio.wait_for(_run_steps(deps, env, definition_json), timeout)
        except asyncio.TimeoutError:
            await _finish(deps, run_id, env.workflow_id, "failed", f"Délai dépassé ({timeout} s).")
        except asyncio.CancelledError:
            await _finish(deps, run_id, env.workflow_id, "failed", "Interrompu (arrêt du serveur).", chain=False)
            raise
        except Exception:
            logger.exception("run de workflow en erreur inattendue", run=run_id)
            await _finish(deps, run_id, env.workflow_id, "failed", "Erreur inattendue.")


async def _run_steps(deps: WorkflowDeps, env: StepEnv, definition_json: str) -> None:
    definition = parse_definition(definition_json)
    run_id, workflow_id = env.run_id, env.workflow_id
    if definition.trigger.type in EVENT_TRIGGER_TYPES:
        # Lancé à la main (« Lancer maintenant »), un workflow par événement n'a pas d'événement : les étapes qui
        # lisent « trigger » doivent pourtant trouver quelque chose.
        env.outputs.setdefault(TRIGGER_REF, "(lancement manuel : aucun événement)")

    for position, step in enumerate(definition.steps):
        started = _now()
        if step.only_if is not None and not guard_allows(env, step.only_if):
            # Condition non remplie : l'étape ne s'exécute pas, son résultat reste vide (les conditions suivantes
            # la voient donc « sans résultat »).
            env.outputs[step.id] = ""
            env.empty.add(step.id)
            await _record_step(deps, run_id, position, step, "skipped", started, output="(condition non remplie)")
            continue
        timeout = AGENT_STEP_TIMEOUT_S if step.type == "agent" else STEP_TIMEOUT_S
        failure: str | None = None
        output = ""
        try:
            output = await asyncio.wait_for(EXECUTORS[step.type](env, step), timeout)
        except StepError as exc:
            failure = str(exc)
        except asyncio.TimeoutError:
            failure = f"délai dépassé ({timeout} s)"
        except Exception:
            logger.exception("étape de workflow en erreur inattendue", step=step.id, type=step.type)
            failure = "erreur inattendue"

        if failure is None:
            env.outputs[step.id] = output
            if is_empty(step.type, output):
                env.empty.add(step.id)
            await _record_step(deps, run_id, position, step, "succeeded", started, output=output, detail=env.traces.get(step.id))
            continue
        if step.optional:
            env.outputs[step.id] = f"(indisponible : {failure})"
            env.empty.add(step.id)
            await _record_step(deps, run_id, position, step, "skipped", started, error=failure, detail=env.traces.get(step.id))
            continue
        await _record_step(deps, run_id, position, step, "failed", started, error=failure, detail=env.traces.get(step.id))
        await _finish(deps, run_id, workflow_id, "failed", f"Étape « {step.id} » : {failure}")
        return

    last = next((env.outputs[s.id] for s in reversed(definition.steps) if env.outputs.get(s.id)), "")
    await _finish(deps, run_id, workflow_id, "succeeded", None, result=last[:FOLLOWER_CONTEXT_CHARS])


async def recover_interrupted_runs(deps: WorkflowDeps) -> int:
    """Au démarrage : un run resté « running » a été coupé par un arrêt du serveur."""
    async with deps.db.session() as session:
        result = await session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.status == "running")
            .values(status="failed", error="Interrompu par un redémarrage du serveur.", finished_at=_now())
        )
        return result.rowcount or 0
