from typing import Any


class ToolkitAPIError(Exception):
    """Base class for all exceptions raised by the Cognite Toolkit API client."""

    def __init__(
        self, message: str, missing: list[dict[str, Any]] | None = None, duplicated: list[dict[str, Any]] | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.missing = missing
        self.duplicated = duplicated
