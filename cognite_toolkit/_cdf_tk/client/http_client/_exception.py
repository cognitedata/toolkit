from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client.http_client import ErrorDetails


class ToolkitAPIError(Exception):
    """Base class for all exceptions raised by the Cognite Toolkit API client."""

    def __init__(
        self,
        message: str,
        missing: list[dict[str, Any]] | None = None,
        duplicated: list[dict[str, Any]] | None = None,
        code: int | None = None,
        error_details: ErrorDetails | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.missing = missing
        self.duplicated = duplicated
        self.code = code
        self.details = error_details
