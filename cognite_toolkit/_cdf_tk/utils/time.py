"""These are taken from the PySDK
The reason for not importing them is that they are private dependencies, thus we have no guarantee that they will
not be moved.
"""

import numbers
import re
import time
from datetime import datetime

UNIT_IN_MS_WITHOUT_WEEK = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
UNIT_IN_MS = {**UNIT_IN_MS_WITHOUT_WEEK, "w": 604800000}
MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999


def time_string_to_ms(pattern: str, string: str, unit_in_ms: dict[str, int]) -> int | None:
    pattern = pattern.format("|".join(unit_in_ms))
    if res := re.fullmatch(pattern, string):
        magnitude = int(res[1])
        unit = res[2]
        return magnitude * unit_in_ms[unit]
    return None


def time_shift_to_ms(time_ago_string: str) -> int:
    """Returns millisecond representation of time-shift string"""
    if time_ago_string == "now":
        return 0
    ms = time_string_to_ms(r"(\d+)({})-(?:ago|ahead)", time_ago_string, UNIT_IN_MS)
    if ms is None:
        raise ValueError(
            f"Invalid time-shift format: `{time_ago_string}`. Must be on format <integer>(s|m|h|d|w)-(ago|ahead) or 'now'. "
            "E.g. '3d-ago' or '1w-ahead'."
        )
    if "ahead" in time_ago_string:
        return -ms
    return ms


def timestamp_to_ms(timestamp: int | float | str | datetime) -> int:
    """Returns the ms representation of some timestamp given by milliseconds, time-shift format or datetime object

    Args:
        timestamp (int | float | str | datetime): Convert this timestamp to ms.

    Returns:
        int: Milliseconds since epoch representation of timestamp

    Examples:

        Gets the millisecond representation of a timestamp:

            >>> from cognite.client.utils import timestamp_to_ms
            >>> from datetime import datetime
            >>> timestamp_to_ms(datetime(2021, 1, 7, 12, 0, 0))
            >>> timestamp_to_ms("now")
            >>> timestamp_to_ms("2w-ago") # 2 weeks ago
            >>> timestamp_to_ms("3d-ahead") # 3 days ahead from now
    """
    if isinstance(timestamp, numbers.Number):  # float, int, int64 etc
        ms = int(timestamp)  # type: ignore[arg-type]
    elif isinstance(timestamp, str):
        ms = round(time.time() * 1000) - time_shift_to_ms(timestamp)
    elif isinstance(timestamp, datetime):
        ms = datetime_to_ms(timestamp)
    else:
        raise TypeError(
            f"Timestamp `{timestamp}` was of type {type(timestamp)}, but must be int, float, str or datetime,"
        )

    if ms < MIN_TIMESTAMP_MS:
        raise ValueError(f"Timestamps must represent a time after 1.1.1900, but {ms} was provided")

    return ms


def datetime_to_ms(dt: datetime) -> int:
    """Converts a datetime object to milliseconds since epoch.

    Args:
        dt (datetime): Naive or aware datetime object. Naive datetimes are interpreted as local time.

    Returns:
        int: Milliseconds since epoch (negative for time prior to 1970-01-01)
    """
    try:
        return int(1000 * dt.timestamp())
    except OSError as e:
        # OSError is raised if dt.timestamp() is called before 1970-01-01 on Windows for naive datetime.
        raise ValueError(
            "Failed to convert datetime to epoch. This likely because you are using a naive datetime. "
            "Try using a timezone aware datetime instead."
        ) from e
