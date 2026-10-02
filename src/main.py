from __future__ import annotations

import asyncio
import contextlib

from fastapi import APIRouter
from xcore.sdk import AutoDispatchMixin, TrustedBase, get_logger

from .engine import WorkflowDeps
from .models import Base
from .routes.workflow_routes import workflows_router
from .runner import run_scheduler

logger = get_logger("workflows.plugin")


class Plugin(AutoDispatchMixin, TrustedBase):
    async def on_load(self) -> None:
        self._db = self.get_service("db")

        async with self._db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

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
