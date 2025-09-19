from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
)

if TYPE_CHECKING:
    from sentry_sdk.types import Event as SentryEvent
    from sentry_sdk.types import Hint as SentryHint


def sentry_exception_filter(event: "SentryEvent", hint: "SentryHint") -> "SentryEvent | None":
    if "exc_info" in hint:
        _exc_type, exc_value, _tb = hint["exc_info"]
        # Returning None prevents the event from being sent to Sentry
        if isinstance(exc_value, ToolkitError):
            return None
    return event
