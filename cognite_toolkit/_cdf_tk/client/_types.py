import re
import time
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, TypeAlias

from pydantic import BeforeValidator, PlainSerializer

from cognite_toolkit._cdf_tk.utils.dms import dms_datetime_iso

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_RELATIVE_TIMESTAMP = re.compile(r"^(\d+)([smhdw])-(ago|ahead)$")
_EPOCH_MS = re.compile(r"^-?\d+$")
_UNIT_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def _keys_and_values_as_string(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    return {str(k): str(v) for k, v in value.items()}


def _datetime_to_epoch_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    delta = value - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _epoch_ms_to_datetime(value: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=value)


def _parse_iso_timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp {value!r}. Use epoch milliseconds, an ISO 8601 string, 'now', "
            "or '<n>(s|m|h|d|w)-(ago|ahead)'."
        ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_timestamp_string(value: str) -> datetime:
    if _EPOCH_MS.fullmatch(value):
        return _epoch_ms_to_datetime(int(value))
    if value == "now":
        return _epoch_ms_to_datetime(int(time.time() * 1000))
    if match := _RELATIVE_TIMESTAMP.fullmatch(value):
        delta_ms = int(match.group(1)) * _UNIT_MS[match.group(2)]
        now_ms = int(time.time() * 1000)
        if match.group(3) == "ago":
            return _epoch_ms_to_datetime(now_ms - delta_ms)
        return _epoch_ms_to_datetime(now_ms + delta_ms)
    return _parse_iso_timestamp(value)


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, bool):
        raise ValueError(f"Invalid timestamp {value!r}.")
    if isinstance(value, int):
        return _epoch_ms_to_datetime(value)
    if isinstance(value, float):
        return _epoch_ms_to_datetime(int(value))
    if isinstance(value, str):
        return _parse_timestamp_string(value)
    raise ValueError(f"Invalid timestamp {value!r}.")


Metadata: TypeAlias = Annotated[dict[str, str], BeforeValidator(_keys_and_values_as_string)]
DMSTimestamp: TypeAlias = Annotated[datetime, PlainSerializer(dms_datetime_iso)]
Timestamp: TypeAlias = Annotated[
    datetime,
    BeforeValidator(_parse_timestamp),
    PlainSerializer(_datetime_to_epoch_ms, return_type=int, when_used="always"),
]
