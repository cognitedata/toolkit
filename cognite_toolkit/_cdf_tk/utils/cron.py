"""Expands cron expressions into the minutes of the week they run at."""

from collections.abc import Mapping
from datetime import datetime

from dateutil import tz

MINUTES_PER_DAY = 24 * 60
MINUTES_PER_WEEK = 7 * MINUTES_PER_DAY

_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
_MONTHS = {
    name: number
    for number, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), start=1
    )
}
_WEEKDAYS = {name: number for number, name in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"))}
_ANY = frozenset({"*", "?"})


def cron_minutes_of_week(expression: str, utc_offset_minutes: int = 0) -> set[int]:
    """The minutes of the week a cron expression runs at, counted in UTC from Monday 00:00.

    The day of month and month are validated but not expanded, as the days they select can fall on any day of
    the week. An expression restricted only by them can thus run on any day of the week.

    Args:
        expression: A five-field cron expression, e.g. '0 6 * * MON-FRI', or a macro such as '@daily'.
        utc_offset_minutes: How many minutes the time zone of the expression is ahead of UTC.

    Returns:
        The minutes of the week, from 0 (Monday 00:00 UTC) to 10079 (Sunday 23:59 UTC).

    Raises:
        ValueError: If the expression is not a valid five-field cron expression.
    """
    fields = _MACROS.get(expression.strip().lower(), expression).split()
    if len(fields) != 5:
        raise ValueError(f"Expected five fields in the cron expression {expression!r}, got {len(fields)}.")
    minute_field, hour_field, day_of_month_field, month_field, weekday_field = fields
    minutes = _expand(minute_field, 0, 59)
    hours = _expand(hour_field, 0, 23)
    _expand(day_of_month_field, 1, 31)
    _expand(month_field, 1, 12, _MONTHS)
    if weekday_field in _ANY or day_of_month_field not in _ANY:
        # Cron runs when either the day of month or the weekday matches, if both are restricted.
        weekdays = set(range(7))
    else:
        # Cron counts weekdays from Sunday (0 or 7), we count from Monday (0).
        weekdays = {(weekday - 1) % 7 for weekday in _expand(weekday_field, 0, 7, _WEEKDAYS)}
    return {
        (weekday * MINUTES_PER_DAY + hour * 60 + minute - utc_offset_minutes) % MINUTES_PER_WEEK
        for weekday in weekdays
        for hour in hours
        for minute in minutes
    }


def utc_offset_minutes(timezone: str | None) -> int:
    """The current offset of the time zone from UTC in minutes. Unknown time zones are treated as UTC."""
    if not timezone or (zone := tz.gettz(timezone)) is None:
        return 0
    offset = datetime.now(zone).utcoffset()
    return int(offset.total_seconds() // 60) if offset is not None else 0


def _expand(field: str, low: int, high: int, names: Mapping[str, int] | None = None) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        range_text, has_step, step_text = part.partition("/")
        step = _parse(step_text) if has_step else 1
        if range_text in _ANY:
            start, end = low, high
        else:
            start_text, has_end, end_text = range_text.partition("-")
            start = _parse(start_text, names)
            # 'a/n' runs from a to the end of the range, 'a' only at a.
            end = _parse(end_text, names) if has_end else (high if has_step else start)
        if step < 1 or not low <= start <= end <= high:
            raise ValueError(f"Invalid cron field {field!r}, values must be between {low} and {high}.")
        values.update(range(start, end + 1, step))
    return values


def _parse(text: str, names: Mapping[str, int] | None = None) -> int:
    if names and text.upper() in names:
        return names[text.upper()]
    if not text.isdigit():
        raise ValueError(f"Invalid cron value {text!r}.")
    return int(text)
