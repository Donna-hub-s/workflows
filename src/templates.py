"""Modèles de workflows prêts à l'emploi : l'utilisateur règle l'heure et les canaux, le reste est fixé."""

from __future__ import annotations

from .definition import Definition, parse_definition

BRIEFING_INSTRUCTION = (
    "Fais mon briefing du matin à partir de ces données : ce qui m'attend aujourd'hui (agenda), les emails qui "
    "méritent mon attention, mes rappels du jour et ce qui attend mon approbation. Va à l'essentiel, signale ce qui "
    "est urgent ou en conflit, et propose une action concrète si quelque chose le justifie. Si une source est "
    "indisponible, dis-le en une phrase."
)


def morning_briefing(*, at: str = "08:00", timezone: str = "UTC", channels: list[str] | None = None, every: str = "weekdays") -> Definition:
    return parse_definition(
        {
            "trigger": {"type": "schedule", "every": every, "at": at, "timezone": timezone},
            "steps": [
                {"id": "agenda", "type": "list_events", "range": "today", "max_results": 15, "optional": True},
                {
                    "id": "emails", "type": "search_emails", "query": "is:unread newer_than:1d is:important",
                    "max_results": 10, "optional": True,
                },
                {"id": "rappels", "type": "list_reminders", "range": "today", "optional": True},
                {"id": "attente", "type": "list_pending_actions", "optional": True},
                {
                    "id": "resume", "type": "llm_summarize", "inputs": ["agenda", "emails", "rappels", "attente"],
                    "instruction": BRIEFING_INSTRUCTION, "max_words": 180,
                },
                {"id": "envoi", "type": "notify", "source": "resume", "title": "Briefing du matin", "channels": channels or ["app"]},
            ],
        }
    )


TEMPLATES = {"morning_briefing": ("Briefing du matin", morning_briefing)}
