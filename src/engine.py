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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, update
from xcore.sdk import get_logger

from .definition import DefinitionError, definition_hash, parse_definition
from .models import Workflow, WorkflowRun, WorkflowStepRun
from .steps import EXECUTORS, StepEnv, StepError

logger = get_logger("workflows.engine")

STEP_TIMEOUT_S = 120
RUN_TIMEOUT_S = 300
USER_RUNS_PER_DAY = 200
STORED_OUTPUT_CHARS = 4000
MAX_CONCURRENT_RUNS = 3

_slots: asyncio.Semaphore | None = None


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


async def create_run(deps: WorkflowDeps, workflow_id: str, trigger: str) -> str | None:
    """Enregistre un run et renvoie son id, ou None si le workflow n'a pas le droit de tourner."""
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

        if trigger == "schedule" and (workflow.status != "active" or workflow.approved_hash != digest):
            # Défense en profondeur : seule une définition approuvée tourne seule.
            logger.warning("run planifié refusé (non actif ou définition modifiée)", workflow=workflow_id)
            return None

        run = WorkflowRun(
            workflow_id=workflow.id,
            tenant_id=workflow.tenant_id,
            user_id=workflow.user_id,
            trigger=trigger,
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
    output: str | None = None, error: str | None = None,
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
                started_at=started,
                finished_at=_now(),
            )
        )


async def _finish(deps: WorkflowDeps, run_id: str, workflow_id: str, status: str, error: str | None) -> None:
    now = _now()
    async with deps.db.session() as session:
        await session.execute(
            update(WorkflowRun).where(WorkflowRun.id == run_id).values(status=status, error=error, finished_at=now)
        )
        await session.execute(update(Workflow).where(Workflow.id == workflow_id).values(last_run_at=now))


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
            definition_json = workflow.definition
        try:
            await asyncio.wait_for(_run_steps(deps, env, definition_json), RUN_TIMEOUT_S)
        except asyncio.TimeoutError:
            await _finish(deps, run_id, env.workflow_id, "failed", f"Délai dépassé ({RUN_TIMEOUT_S} s).")
        except asyncio.CancelledError:
            await _finish(deps, run_id, env.workflow_id, "failed", "Interrompu (arrêt du serveur).")
            raise
        except Exception:
            logger.exception("run de workflow en erreur inattendue", run=run_id)
            await _finish(deps, run_id, env.workflow_id, "failed", "Erreur inattendue.")


async def _run_steps(deps: WorkflowDeps, env: StepEnv, definition_json: str) -> None:
    definition = parse_definition(definition_json)
    run_id, workflow_id = env.run_id, env.workflow_id

    for position, step in enumerate(definition.steps):
        started = _now()
        failure: str | None = None
        output = ""
        try:
            output = await asyncio.wait_for(EXECUTORS[step.type](env, step), STEP_TIMEOUT_S)
        except StepError as exc:
            failure = str(exc)
        except asyncio.TimeoutError:
            failure = f"délai dépassé ({STEP_TIMEOUT_S} s)"
        except Exception:
            logger.exception("étape de workflow en erreur inattendue", step=step.id, type=step.type)
            failure = "erreur inattendue"

        if failure is None:
            env.outputs[step.id] = output
            await _record_step(deps, run_id, position, step, "succeeded", started, output=output)
            continue
        if step.optional:
            env.outputs[step.id] = f"(indisponible : {failure})"
            await _record_step(deps, run_id, position, step, "skipped", started, error=failure)
            continue
        await _record_step(deps, run_id, position, step, "failed", started, error=failure)
        await _finish(deps, run_id, workflow_id, "failed", f"Étape « {step.id} » : {failure}")
        return

    await _finish(deps, run_id, workflow_id, "succeeded", None)


async def recover_interrupted_runs(deps: WorkflowDeps) -> int:
    """Au démarrage : un run resté « running » a été coupé par un arrêt du serveur."""
    async with deps.db.session() as session:
        result = await session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.status == "running")
            .values(status="failed", error="Interrompu par un redémarrage du serveur.", finished_at=_now())
        )
        return result.rowcount or 0
