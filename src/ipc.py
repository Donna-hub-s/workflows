from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TypedDict

from sqlalchemy import select
from xcore.sdk import AutoDispatchMixin, action, error, ok, schema, validate_payload

from .definition import DefinitionError, canonical_json, default_max_runs, definition_hash, parse_definition
from .models import Workflow
from .service import activate_workflow, describe, schedule_text
from .templates import TEMPLATES

MAX_WORKFLOWS_PER_USER = 50

PROPOSE_SCHEMA: TypedDict = {  # type: ignore
    "tenant_id": (str, ...),
    "user_id": (str, ...),
    "name": (str, ...),
    "description": (str, None),
    "definition": (dict, None),  # soit une définition complète…
    "template": (str, None),  # …soit un modèle (ex. morning_briefing) réglé par template_options
    "template_options": (dict, None),
}

ACTIVATE_SCHEMA: TypedDict = {  # type: ignore
    "tenant_id": (str, ...),
    "user_id": (str, ...),
    "workflow_id": (str, ...),
    "expected_hash": (str, ...),  # l'empreinte que l'utilisateur a vue et approuvée
}

OWNED_SCHEMA: TypedDict = {  # type: ignore
    "tenant_id": (str, ...),
    "user_id": (str, ...),
    "workflow_id": (str, ...),
}

LIST_SCHEMA: TypedDict = {  # type: ignore
    "tenant_id": (str, ...),
    "user_id": (str, ...),
}


class IPCCommands(AutoDispatchMixin):
    """Actions IPC du plugin workflows, appelées par le plugin chat (outils de DONNA). La classe hôte fournit
    `self._db`.

    DONNA ne peut ici que PROPOSER (créer un brouillon) et METTRE EN PAUSE (retirer une capacité). L'activation
    (`workflows.activate`) n'est appelée par le chat que quand l'UTILISATEUR confirme l'action en attente, et
    exige l'empreinte exacte qu'il a vue : si le brouillon a changé entre-temps, elle est refusée."""

    async def _owned(self, session, payload) -> Workflow | None:
        workflow = await session.get(Workflow, payload.workflow_id)
        if (
            workflow is None
            or workflow.status == "archived"
            or workflow.user_id != payload.user_id
            or workflow.tenant_id != payload.tenant_id
        ):
            return None
        return workflow

    @action("workflows.propose")
    @validate_payload(PROPOSE_SCHEMA, type_response="model", unset=False)  # type: ignore
    async def _ipc_propose(self, payload) -> dict:
        template = getattr(payload, "template", None)
        raw = getattr(payload, "definition", None)
        try:
            if template:
                if template not in TEMPLATES:
                    return error(f"Modèle inconnu : {template}. Modèles : {', '.join(TEMPLATES)}.", code="invalid")
                _, build = TEMPLATES[template]
                definition = build(**(getattr(payload, "template_options", None) or {}))
            elif raw:
                definition = parse_definition(raw)
            else:
                return error("Fournis `template` ou `definition`.", code="invalid")
        except (DefinitionError, TypeError) as exc:
            return error(str(exc) if isinstance(exc, DefinitionError) else f"Options de modèle invalides : {exc}", code="invalid")

        async with self._db.session() as session:  # type: ignore
            existing = (
                await session.execute(
                    select(Workflow.id).where(
                        Workflow.user_id == payload.user_id, Workflow.tenant_id == payload.tenant_id, Workflow.status != "archived"
                    )
                )
            ).all()
            if len(existing) >= MAX_WORKFLOWS_PER_USER:
                return error(f"{MAX_WORKFLOWS_PER_USER} workflows maximum : l'utilisateur doit en archiver un.", code="limit")
            workflow = Workflow(
                tenant_id=payload.tenant_id, user_id=payload.user_id, name=payload.name[:120],
                description=(getattr(payload, "description", None) or None), definition=canonical_json(definition),
                status="draft", origin="donna", max_runs_per_day=default_max_runs(definition),
            )
            session.add(workflow)
            await session.flush()
            return ok(
                workflow_id=workflow.id, name=workflow.name, definition_hash=definition_hash(definition),
                schedule=schedule_text(definition.trigger), lines=describe(definition),
            )

    @action("workflows.activate")
    @validate_payload(ACTIVATE_SCHEMA, type_response="model", unset=False)  # type: ignore
    async def _ipc_activate(self, payload) -> dict:
        async with self._db.session() as session:  # type: ignore
            workflow = await self._owned(session, payload)
            if workflow is None:
                return error("Workflow introuvable (supprimé depuis la proposition ?).", code="not_found")
            try:
                definition = parse_definition(json.loads(workflow.definition))
            except (ValueError, DefinitionError) as exc:
                return error(f"Définition invalide : {exc}", code="invalid")
            if definition_hash(definition) != payload.expected_hash:
                return error("Le workflow a été modifié depuis la proposition : relis-le puis réapprouve-le.", code="changed")
            activate_workflow(workflow, definition, datetime.now(timezone.utc))
            await session.flush()
            return ok(name=workflow.name, schedule=schedule_text(definition.trigger), next_run_at=workflow.next_run_at.isoformat() if workflow.next_run_at else None)

    @action("workflows.discard")
    @validate_payload(OWNED_SCHEMA, type_response="model", unset=False)  # type: ignore
    async def _ipc_discard(self, payload) -> dict:
        """Refus de l'utilisateur : le brouillon proposé par DONNA est archivé. Un workflow déjà approuvé n'est jamais touché."""
        async with self._db.session() as session:  # type: ignore
            workflow = await self._owned(session, payload)
            if workflow is None or workflow.status != "draft" or workflow.approved_hash is not None:
                return ok(archived=False)
            workflow.status, workflow.next_run_at = "archived", None
            return ok(archived=True)

    @action("workflows.list", side_effect="read")
    @schema(
        version="1.0", input=LIST_SCHEMA, description="Liste les workflows DE l'appelant.",
        type_response="model", unset=False,
    )  # type: ignore
    async def _ipc_list(self, payload) -> dict:
        async with self._db.session() as session:  # type: ignore
            rows = (
                await session.execute(
                    select(Workflow)
                    .where(Workflow.user_id == payload.user_id, Workflow.tenant_id == payload.tenant_id, Workflow.status != "archived")
                    .order_by(Workflow.created_at.desc())
                    .limit(50)
                )
            ).scalars().all()
            items = []
            for row in rows:
                try:
                    schedule = schedule_text(parse_definition(json.loads(row.definition)).trigger)
                except (ValueError, DefinitionError):
                    schedule = "définition illisible"
                items.append({
                    "id": row.id, "name": row.name, "status": row.status, "schedule": schedule,
                    "next_run_at": row.next_run_at.isoformat() if row.next_run_at else None,
                })
            return ok(workflows=items)

    @action("workflows.pause", side_effect="write")
    @schema(
        version="1.0", input=OWNED_SCHEMA,
        description="Met en pause un workflow DE l'appelant — retirer une capacité ne demande pas d'approbation.",
        type_response="model", unset=False,
    )  # type: ignore
    async def _ipc_pause(self, payload) -> dict:
        """Mettre en pause RETIRE une capacité : DONNA peut le faire sans approbation (jamais l'inverse)."""
        async with self._db.session() as session:  # type: ignore
            workflow = await self._owned(session, payload)
            if workflow is None:
                return error("Workflow introuvable.", code="not_found")
            workflow.status, workflow.next_run_at = "paused", None
            return ok(name=workflow.name)
