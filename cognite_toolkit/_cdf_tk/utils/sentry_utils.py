from typing import TYPE_CHECKING, Optional

from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
)

if TYPE_CHECKING:
    from sentry_sdk.types import Event as SentryEvent
    from sentry_sdk.types import Hint as SentryHint


def sentry_exception_filter(event: "SentryEvent", hint: "SentryHint") -> "Optional[SentryEvent]":
    if "exc_info" in hint:
        exc_type, exc_value, tb = hint["exc_info"]
        # Returning None prevents the event from being sent to Sentry
        if isinstance(exc_value, ToolkitError):
            return None
    return event
