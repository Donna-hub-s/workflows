from __future__ import annotations

import asyncio
import contextlib

from fastapi import APIRouter
from xcore.sdk import AutoDispatchMixin, TrustedBase, get_logger

from extensions.commons.db_columns import ensure_columns

from .engine import WorkflowDeps
from .ipc import IPCCommands
from .models import Base
from .routes.workflow_routes import workflows_router
from .runner import run_scheduler

logger = get_logger("workflows.plugin")


class Plugin(IPCCommands, AutoDispatchMixin, TrustedBase):
    async def on_load(self) -> None:
        self._db = self.get_service("db")

        async with self._db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # create_all ne modifie jamais une table existante : nouvelles colonnes par ALTER TABLE.
            await ensure_columns(conn, "workflows_step_runs", (("detail", "TEXT", "TEXT"),), logger)
            await ensure_columns(conn, "workflows_workflows", (("state", "TEXT", "TEXT"),), logger)
            await ensure_columns(
                conn, "workflows_runs", (("trigger_context", "TEXT", "TEXT"), ("depth", "INTEGER NOT NULL DEFAULT 0", "INTEGER NOT NULL DEFAULT 0")), logger
            )

        # Tout ce qui demande Google, le modèle ou WhatsApp passe par l'IPC du plugin chat (steps.py).
        self._deps = WorkflowDeps(db=self._db, call_plugin=self.call_plugin)

        self.app = APIRouter()
        self.app.include_router(workflows_router(self._db, self._deps))

        # Planificateur : processus API uniquement (les étapes ont besoin de l'IPC, absent d'un worker Celery).
        self._scheduler_task = asyncio.create_task(run_scheduler(self._deps))
        logger.info("workflows plugin prêt")

    async def on_unload(self) -> None:
        self._scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._scheduler_task

    def get_router(self) -> APIRouter | None:
        return self.app
