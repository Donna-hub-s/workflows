from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Workflow(Base):
    """Automatisation validée par l'utilisateur : un déclencheur et des étapes (voir definition.py).

    `definition` est le JSON canonique. `approved_hash` est l'empreinte que l'utilisateur a approuvée : un
    workflow ne tourne en planification que si la définition courante a encore cette empreinte (toute
    modification repasse en brouillon). DONNA peut proposer un workflow, jamais l'activer."""

    __tablename__ = "workflows_workflows"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(120))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    definition: Mapped[str] = mapped_column(Text)
    # draft (jamais approuvé ou modifié depuis) | active | paused | archived
    status: Mapped[str] = mapped_column(String(16), default="draft", index=True)
    # user (créé par l'utilisateur) | template (modèle prêt à l'emploi) | donna (proposé par DONNA, approuvé)
    origin: Mapped[str] = mapped_column(String(16), default="user")
    approved_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Prochaine exécution planifiée (UTC), NULL pour un déclencheur manuel ou un workflow non actif.
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    max_runs_per_day: Mapped[int] = mapped_column(Integer, default=24)
    # Conversation où la notification « application » dépose ses messages (créée par le chat à la première).
    conversation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # JSON : état d'un déclencheur par événement (emails déjà vus…) — colonne ajoutée par ALTER TABLE (voir main.py).
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class WorkflowRun(Base):
    __tablename__ = "workflows_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    workflow_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflows_workflows.id"), index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    # running | succeeded | failed | skipped (plafond atteint, retard trop grand…)
    status: Mapped[str] = mapped_column(String(16), default="running", index=True)
    trigger: Mapped[str] = mapped_column(String(16))  # schedule | manual | event
    # Ce qui a déclenché un run par événement (emails reçus, workflow terminé…) : la sortie « trigger » des étapes.
    # Colonnes ajoutées par ALTER TABLE (voir main.py).
    trigger_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    depth: Mapped[int] = mapped_column(Integer, default=0)  # profondeur d'enchaînement (workflow_finished)
    definition_hash: Mapped[str] = mapped_column(String(64))
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WorkflowStepRun(Base):
    __tablename__ = "workflows_step_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflows_runs.id"), index=True)
    position: Mapped[int] = mapped_column(Integer)
    step_id: Mapped[str] = mapped_column(String(32))
    type: Mapped[str] = mapped_column(String(32))
    # succeeded | failed | skipped (étape facultative indisponible)
    status: Mapped[str] = mapped_column(String(16))
    # Sortie tronquée (historique) ; la sortie complète ne sert qu'aux étapes suivantes, en mémoire.
    output: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # JSON : journal des outils appelés par une étape « agent » (nom, arguments, début du résultat) — colonne
    # ajoutée par ALTER TABLE (voir main.py).
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
