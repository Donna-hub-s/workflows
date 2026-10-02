from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    definition: dict
    # None = plafond par défaut selon la définition (voir definition.default_max_runs : plus bas avec un agent).
    max_runs_per_day: int | None = Field(default=None, ge=1, le=96)


class WorkflowPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    definition: dict | None = None
    max_runs_per_day: int | None = Field(default=None, ge=1, le=96)


class FromTemplate(BaseModel):
    template: Literal["morning_briefing"]
    name: str | None = Field(default=None, min_length=1, max_length=120)
    at: str = "08:00"
    timezone: str = "UTC"
    every: Literal["day", "weekdays", "week"] = "weekdays"
    channels: list[Literal["app", "whatsapp", "email"]] = Field(default_factory=lambda: ["app"], min_length=1)


class TemplateOut(BaseModel):
    id: str
    name: str


class WorkflowOut(BaseModel):
    id: str
    name: str
    description: str | None
    status: str
    origin: str
    definition: dict
    approved: bool  # la définition courante est celle que l'utilisateur a approuvée
    schedule: str  # déclencheur en clair (« Chaque jour ouvré à 08:00 (Europe/Paris) »)
    lines: list[str]  # ce que l'utilisateur approuve : le déclencheur puis une ligne par étape
    next_run_at: datetime | None
    last_run_at: datetime | None
    max_runs_per_day: int
    conversation_id: str | None
    created_at: datetime
    updated_at: datetime


class StepRunOut(BaseModel):
    position: int
    step_id: str
    type: str
    status: str
    output: str | None
    error: str | None
    # Étape « agent » : outils appelés, dans l'ordre ({name, arguments, result}) — pour voir ce que DONNA a fait.
    detail: list[dict] | None = None


class RunOut(BaseModel):
    id: str
    status: str
    trigger: str
    error: str | None
    started_at: datetime
    finished_at: datetime | None
    steps: list[StepRunOut] = Field(default_factory=list)


class RunStarted(BaseModel):
    run_id: str
