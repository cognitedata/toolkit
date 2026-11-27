"""Expands cron expressions into the times they run at."""

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta

from dateutil import tz

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


def cron_starts(expression: str, start: datetime, end: datetime, timezone: str | None = None) -> list[datetime]:
    """The times a cron expression runs at from start (inclusive) to end (exclusive).

    If both the day of month and the weekday are restricted, cron runs when either matches. Local times that do not
    exist because of a daylight saving time change are moved forward to the first time that exists.

    Args:
        expression: A five-field cron expression, e.g. '0 6 * * MON-FRI', or a macro such as '@daily'.
        start: The start of the window, a time zone aware datetime.
        end: The end of the window, a time zone aware datetime.
        timezone: The IANA time zone the expression is evaluated in, e.g. 'Europe/Oslo'. Unknown time zones and
            None are treated as UTC.

    Returns:
        The start times in UTC, sorted.

    Raises:
        ValueError: If the expression is not a valid five-field cron expression.
    """
    fields = _MACROS.get(expression.strip().lower(), expression).split()
    if len(fields) != 5:
        raise ValueError(f"Expected five fields in the cron expression {expression!r}, got {len(fields)}.")
    minute_field, hour_field, day_of_month_field, month_field, weekday_field = fields
    minutes = sorted(_expand(minute_field, 0, 59))
    hours = sorted(_expand(hour_field, 0, 23))
    days_of_month = _expand(day_of_month_field, 1, 31)
    months = _expand(month_field, 1, 12, _MONTHS)
    # Cron counts weekdays from Sunday as 0 or 7, isoweekday from Monday as 1 to Sunday as 7.
    weekdays = {weekday % 7 for weekday in _expand(weekday_field, 0, 7, _WEEKDAYS)}
    either_day = day_of_month_field not in _ANY and weekday_field not in _ANY

    def runs_on(day: date) -> bool:
        if day.month not in months:
            return False
        day_of_month_matches = day.day in days_of_month
        weekday_matches = day.isoweekday() % 7 in weekdays
        if either_day:
            return day_of_month_matches or weekday_matches
        return day_of_month_matches and weekday_matches

    zone = (tz.gettz(timezone) if timezone else None) or UTC
    starts: set[datetime] = set()
    day = start.astimezone(zone).date()
    last_day = end.astimezone(zone).date()
    while day <= last_day:
        if runs_on(day):
            for hour in hours:
                for minute in minutes:
                    local = tz.resolve_imaginary(datetime(day.year, day.month, day.day, hour, minute, tzinfo=zone))
                    if start <= (run := local.astimezone(UTC)) < end:
                        starts.add(run)
        day += timedelta(days=1)
    return sorted(starts)


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
