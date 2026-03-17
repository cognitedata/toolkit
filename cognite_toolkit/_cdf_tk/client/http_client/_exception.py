from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client.http_client._data_classes import RequestMessage


class ToolkitAPIError(Exception):
    """Base class for all exceptions raised by the Cognite Toolkit API client."""

    def __init__(
        self,
        message: str,
        missing: list[dict[str, Any]] | None = None,
        duplicated: list[dict[str, Any]] | None = None,
        code: int | None = None,
        is_auto_retryable: bool | None = None,
        request: "RequestMessage | None " = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.missing = missing
        self.duplicated = duplicated
        self.code = code
        self.is_auto_retryable = is_auto_retryable
        self.request = request
