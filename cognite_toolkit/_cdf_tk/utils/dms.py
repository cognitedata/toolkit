from datetime import date, datetime, timezone


def serialize_dms(
    value: str | int | float | bool | dict | list | datetime | date | None,
) -> str | int | float | bool | dict | list | None:
    """Serializes a value to a format that can be stored in DMS.

    Args:
        value: The value to serialize.
    Returns:
        The serialized value.
    """
    if isinstance(value, datetime):
        return _dms_datetime_iso(value)
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, list):
        return [serialize_dms(v) for v in value]
    else:
        return value


def _dms_datetime_iso(value: datetime) -> str:
    """Serializes a datetime value to ISO format for DMS.

    Note that .isoformat() does not create the correct format.

    Args:
        value: The datetime value to serialize.
    Returns:
        The serialized datetime in ISO format.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    dt_utc = value.astimezone(timezone.utc)
    iso_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%S")

    # Add milliseconds with max 3 digits precision
    millis = dt_utc.microsecond // 1000
    if millis > 0:
        iso_string += f".{millis:03d}"
    # Add Z for UTC
    iso_string += "Z"
    return iso_string
