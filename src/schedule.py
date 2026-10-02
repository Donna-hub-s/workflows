"""Prochaine exécution d'un déclencheur planifié — pure (aucune E/S), toujours en UTC.

Le calcul se fait dans le fuseau de l'utilisateur : « tous les jours à 08:00 » reste 08:00 locales à travers
un changement d'heure. Les heures inexistantes (saut de printemps) sont décalées par zoneinfo plutôt que sautées.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .definition import WEEKDAYS, ScheduleTrigger

_WEEKDAYS_ONLY = ("mon", "tue", "wed", "thu", "fri")


def _allowed_days(trigger: ScheduleTrigger) -> tuple[str, ...]:
    if trigger.every == "day":
        return WEEKDAYS
    if trigger.every == "weekdays":
        return _WEEKDAYS_ONLY
    return tuple(trigger.days) or ("mon",)


def aware_utc(value: datetime) -> datetime:
    """SQLite rend des datetimes naïfs (toujours stockés en UTC dans ce projet)."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def next_run_after(trigger: ScheduleTrigger, after: datetime) -> datetime:
    """Première occurrence STRICTEMENT postérieure à `after`, en UTC."""
    zone = ZoneInfo(trigger.timezone)
    after_utc = aware_utc(after)
    hour, minute = (int(part) for part in trigger.at.split(":"))
    allowed = _allowed_days(trigger)
    first_day: date = after_utc.astimezone(zone).date()
    # 8 jours suffisent : une semaine complète plus le jour courant.
    for offset in range(8):
        day = first_day + timedelta(days=offset)
        if WEEKDAYS[day.weekday()] not in allowed:
            continue
        candidate = datetime.combine(day, time(hour, minute), tzinfo=zone).astimezone(timezone.utc)
        if candidate > after_utc:
            return candidate
    raise RuntimeError("aucune occurrence trouvée")  # inatteignable : `allowed` n'est jamais vide
