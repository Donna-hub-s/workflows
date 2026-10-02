"""Boucle de planification : toutes les 30 s, lance les workflows actifs dont l'heure est venue.

Tourne dans le processus API (comme les ponts de rappel) : les étapes passent par l'IPC du plugin chat, qui n'existe pas dans un worker Celery.
Chaque workflow est « réclamé » par un UPDATE conditionnel (« toujours échu » → avance next_run_at à l'occurrence
suivante) : si deux passages se croisent, le second ne trouve plus rien d'échu, donc une échéance ne produit qu'un
run. On compare avec <= now et non avec l'ancienne valeur : l'égalité de deux datetimes sérialisés en texte est
fragile (une valeur écrite sans microsecondes ne serait jamais « égale »).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from xcore.sdk import get_logger

from .models import Workflow, WorkflowRun
from .definition import DefinitionError, ScheduleTrigger, parse_definition
from .engine import WorkflowDeps, create_run, execute_run, recover_interrupted_runs
from .schedule import aware_utc, next_run_after

logger = get_logger("workflows.runner")

TICK_SECONDS = 30
BATCH = 20
# Un briefing de 8 h livré à 14 h n'a plus de sens : au-delà de ce retard (serveur éteint, redémarrage), on saute.
CATCHUP = timedelta(hours=2)


async def tick(deps: WorkflowDeps, spawn, now: datetime | None = None) -> list[str]:
    """Un passage de la boucle. `spawn(coro)` lance l'exécution en tâche de fond. Renvoie les ids de runs créés."""
    now = now or datetime.now(timezone.utc)
    started: list[str] = []
    async with deps.db.session() as session:
        due = (
            await session.execute(
                select(Workflow.id, Workflow.next_run_at, Workflow.definition)
                .where(Workflow.status == "active", Workflow.next_run_at.is_not(None), Workflow.next_run_at <= now)
                .order_by(Workflow.next_run_at)
                .limit(BATCH)
            )
        ).all()

    for workflow_id, scheduled, definition_json in due:
        try:
            trigger = parse_definition(definition_json).trigger
        except DefinitionError:
            trigger = None
        if not isinstance(trigger, ScheduleTrigger):
            async with deps.db.session() as session:  # plus rien à planifier
                await session.execute(update(Workflow).where(Workflow.id == workflow_id).values(next_run_at=None))
            continue

        following = next_run_after(trigger, now)
        async with deps.db.session() as session:
            claimed = (
                await session.execute(
                    update(Workflow)
                    .where(Workflow.id == workflow_id, Workflow.status == "active", Workflow.next_run_at <= now)
                    .values(next_run_at=following)
                )
            ).rowcount
        if claimed != 1:
            continue  # déjà pris (ou modifié entre-temps)

        late = now - aware_utc(scheduled)
        if late > CATCHUP:
            await _record_skipped(deps, workflow_id, f"Exécution ignorée : {int(late.total_seconds() // 60)} min de retard.")
            continue
        run_id = await create_run(deps, workflow_id, "schedule")
        if run_id:
            started.append(run_id)
            spawn(execute_run(deps, run_id))
    return started


async def _record_skipped(deps: WorkflowDeps, workflow_id: str, reason: str) -> None:
    async with deps.db.session() as session:
        workflow = await session.get(Workflow, workflow_id)
        if workflow is None:
            return
        now = datetime.now(timezone.utc)
        session.add(
            WorkflowRun(
                workflow_id=workflow.id, tenant_id=workflow.tenant_id, user_id=workflow.user_id, status="skipped",
                trigger="schedule", definition_hash=workflow.approved_hash or "", error=reason,
                started_at=now, finished_at=now,
            )
        )


async def run_scheduler(deps: WorkflowDeps) -> None:
    """À lancer comme tâche de fond (on_load), annulée à on_unload."""
    tasks: set[asyncio.Task] = set()

    def spawn(coro) -> None:
        task = asyncio.create_task(coro)
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    try:
        recovered = await recover_interrupted_runs(deps)
        if recovered:
            logger.warning("runs interrompus par un redémarrage marqués en échec", nombre=recovered)
        while True:
            try:
                await tick(deps, spawn)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("passage du planificateur de workflows en échec")
            await asyncio.sleep(TICK_SECONDS)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
