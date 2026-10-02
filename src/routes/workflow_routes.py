"""API REST des workflows de l'utilisateur connecté (préfixe /app/workflows).

Tout est cloisonné par (tenant, utilisateur) : un workflow d'un autre utilisateur répond 404. L'ACTIVATION est
un geste explicite de l'utilisateur (POST /{id}/activate) : elle enregistre l'empreinte approuvée ; modifier
la définition d'un workflow repasse en brouillon, il faut le ré-approuver. DONNA ne dispose d'aucune route
ici : ses outils (voir le plugin chat) ne peuvent créer que des brouillons.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from xcore.kernel.api import AuthPayload, get_current_user

from ..definition import DefinitionError, canonical_json, definition_hash, parse_definition
from ..engine import WorkflowDeps, create_run, execute_run
from ..models import Workflow, WorkflowRun, WorkflowStepRun
from ..schemas import (
    FromTemplate,
    RunOut,
    RunStarted,
    StepRunOut,
    TemplateOut,
    WorkflowCreate,
    WorkflowOut,
    WorkflowPatch,
)
from ..service import activate_workflow
from ..templates import TEMPLATES

MAX_WORKFLOWS_PER_USER = 50
RUNS_SHOWN = 20

_background: set[asyncio.Task] = set()


def _tenant_of(current_user: AuthPayload) -> str:
    tenant_id = (current_user.get("user") or {}).get("tenant_id")
    if not tenant_id:
        raise HTTPException(403, "Aucun espace associé à ce compte.")
    return tenant_id


def _to_out(workflow: Workflow) -> WorkflowOut:
    try:
        definition = json.loads(workflow.definition)
        approved = workflow.approved_hash is not None and workflow.approved_hash == definition_hash(parse_definition(definition))
    except (ValueError, DefinitionError):
        definition, approved = {}, False
    return WorkflowOut(
        id=workflow.id, name=workflow.name, description=workflow.description, status=workflow.status,
        origin=workflow.origin, definition=definition, approved=approved, next_run_at=workflow.next_run_at,
        last_run_at=workflow.last_run_at, max_runs_per_day=workflow.max_runs_per_day,
        conversation_id=workflow.conversation_id, created_at=workflow.created_at, updated_at=workflow.updated_at,
    )


def _parsed(raw: dict):
    try:
        return parse_definition(raw)
    except DefinitionError as exc:
        raise HTTPException(422, str(exc)) from exc


def workflows_router(db: Any, deps: WorkflowDeps) -> APIRouter:
    router = APIRouter(tags=["workflows"])

    async def _owned(session, workflow_id: str, current_user: AuthPayload) -> Workflow:
        workflow = await session.get(Workflow, workflow_id)
        if (
            workflow is None
            or workflow.status == "archived"
            or workflow.user_id != current_user["sub"]
            or workflow.tenant_id != _tenant_of(current_user)
        ):
            raise HTTPException(404, "Workflow introuvable.")
        return workflow

    async def _create(current_user: AuthPayload, name: str, description: str | None, definition, origin: str, max_runs: int = 24) -> WorkflowOut:
        tenant_id = _tenant_of(current_user)
        async with db.session() as session:
            count = len(
                (
                    await session.execute(
                        select(Workflow.id).where(
                            Workflow.user_id == current_user["sub"], Workflow.tenant_id == tenant_id, Workflow.status != "archived"
                        )
                    )
                ).all()
            )
            if count >= MAX_WORKFLOWS_PER_USER:
                raise HTTPException(409, f"{MAX_WORKFLOWS_PER_USER} workflows maximum : archive-en un avant d'en créer un autre.")
            workflow = Workflow(
                tenant_id=tenant_id, user_id=current_user["sub"], name=name, description=description,
                definition=canonical_json(definition), status="draft", origin=origin, max_runs_per_day=max_runs,
            )
            session.add(workflow)
            await session.flush()
            return _to_out(workflow)

    @router.get("/templates", summary="Modèles de workflows", response_model=list[TemplateOut])
    async def list_templates(current_user: AuthPayload = Depends(get_current_user)) -> list[TemplateOut]:
        return [TemplateOut(id=key, name=name) for key, (name, _build) in TEMPLATES.items()]

    @router.post("/from-template", summary="Créer un brouillon depuis un modèle", response_model=WorkflowOut, status_code=201)
    async def from_template(body: FromTemplate, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        name, build = TEMPLATES[body.template]
        try:
            definition = build(at=body.at, timezone=body.timezone, channels=list(body.channels), every=body.every)
        except DefinitionError as exc:
            raise HTTPException(422, str(exc)) from exc
        return await _create(current_user, body.name or name, None, definition, "template")

    @router.get("/", summary="Mes workflows", response_model=list[WorkflowOut])
    async def list_workflows(current_user: AuthPayload = Depends(get_current_user)) -> list[WorkflowOut]:
        async with db.session() as session:
            rows = (
                await session.execute(
                    select(Workflow)
                    .where(
                        Workflow.user_id == current_user["sub"], Workflow.tenant_id == _tenant_of(current_user),
                        Workflow.status != "archived",
                    )
                    .order_by(Workflow.created_at.desc())
                )
            ).scalars().all()
            return [_to_out(row) for row in rows]

    @router.post("/", summary="Créer un workflow (brouillon)", response_model=WorkflowOut, status_code=201)
    async def create_workflow(body: WorkflowCreate, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        return await _create(current_user, body.name, body.description, _parsed(body.definition), "user", body.max_runs_per_day)

    @router.get("/{workflow_id}", summary="Détail d'un workflow", response_model=WorkflowOut)
    async def get_workflow(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        async with db.session() as session:
            return _to_out(await _owned(session, workflow_id, current_user))

    @router.patch("/{workflow_id}", summary="Modifier un workflow", response_model=WorkflowOut)
    async def patch_workflow(workflow_id: str, body: WorkflowPatch, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        new_definition = _parsed(body.definition) if body.definition is not None else None
        async with db.session() as session:
            workflow = await _owned(session, workflow_id, current_user)
            if body.name is not None:
                workflow.name = body.name
            if body.description is not None:
                workflow.description = body.description
            if body.max_runs_per_day is not None:
                workflow.max_runs_per_day = body.max_runs_per_day
            if new_definition is not None and definition_hash(new_definition) != workflow.approved_hash:
                # Ce que l'utilisateur avait approuvé n'est plus ce qui est stocké : retour en brouillon.
                workflow.definition = canonical_json(new_definition)
                workflow.status, workflow.approved_hash, workflow.approved_at, workflow.next_run_at = "draft", None, None, None
            await session.flush()
            return _to_out(workflow)

    @router.post("/{workflow_id}/activate", summary="Approuver et activer", response_model=WorkflowOut)
    async def activate(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        async with db.session() as session:
            workflow = await _owned(session, workflow_id, current_user)
            activate_workflow(workflow, _parsed(json.loads(workflow.definition)), datetime.now(timezone.utc))
            await session.flush()
            return _to_out(workflow)

    @router.post("/{workflow_id}/pause", summary="Mettre en pause", response_model=WorkflowOut)
    async def pause(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> WorkflowOut:
        async with db.session() as session:
            workflow = await _owned(session, workflow_id, current_user)
            workflow.status, workflow.next_run_at = "paused", None
            await session.flush()
            return _to_out(workflow)

    @router.delete("/{workflow_id}", summary="Archiver un workflow", status_code=204)
    async def archive(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> None:
        async with db.session() as session:
            workflow = await _owned(session, workflow_id, current_user)
            workflow.status, workflow.next_run_at = "archived", None

    @router.post("/{workflow_id}/run", summary="Lancer maintenant", response_model=RunStarted, status_code=202)
    async def run_now(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> RunStarted:
        async with db.session() as session:
            await _owned(session, workflow_id, current_user)  # cloisonnement avant tout
        run_id = await create_run(deps, workflow_id, "manual")
        if run_id is None:
            raise HTTPException(409, "Ce workflow ne peut pas être lancé.")
        task = asyncio.create_task(execute_run(deps, run_id))
        _background.add(task)
        task.add_done_callback(_background.discard)
        return RunStarted(run_id=run_id)

    @router.get("/{workflow_id}/runs", summary="Historique des exécutions", response_model=list[RunOut])
    async def runs(workflow_id: str, current_user: AuthPayload = Depends(get_current_user)) -> list[RunOut]:
        async with db.session() as session:
            await _owned(session, workflow_id, current_user)
            rows = (
                await session.execute(
                    select(WorkflowRun).where(WorkflowRun.workflow_id == workflow_id).order_by(WorkflowRun.started_at.desc()).limit(RUNS_SHOWN)
                )
            ).scalars().all()
            out: list[RunOut] = []
            for run in rows:
                steps = (
                    await session.execute(
                        select(WorkflowStepRun).where(WorkflowStepRun.run_id == run.id).order_by(WorkflowStepRun.position)
                    )
                ).scalars().all()
                out.append(
                    RunOut(
                        id=run.id, status=run.status, trigger=run.trigger, error=run.error, started_at=run.started_at,
                        finished_at=run.finished_at,
                        steps=[
                            StepRunOut(position=s.position, step_id=s.step_id, type=s.type, status=s.status, output=s.output, error=s.error)
                            for s in steps
                        ],
                    )
                )
            return out

    return router
