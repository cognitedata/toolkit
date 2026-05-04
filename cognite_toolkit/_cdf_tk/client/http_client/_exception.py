from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from cognite.client.exceptions import CogniteAPIError

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse, RequestMessage


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
        response: "FailedResponse | None " = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.missing = missing
        self.duplicated = duplicated
        self.code = code
        self.is_auto_retryable = is_auto_retryable
        self.request = request
        self.response = response

    def as_debug_dict(self) -> dict[str, Any]:
        debug_info: dict[str, Any] = {}
        if self.request:
            debug_info["url"] = self.request.endpoint_url
            debug_info["method"] = self.request.method
            if self.request.parameters:
                debug_info["requestParameters"] = self.request.parameters
            if self.request.body_content:
                debug_info["requestBody"] = self.request.body_content
            elif self.request.data_content:
                debug_info["requestBody"] = "<bytes>"
        debug_info["errorMessage"] = self.message
        if self.code:
            debug_info["statusCode"] = self.code
        if self.is_auto_retryable is not None:
            debug_info["isAutoRetryable"] = self.is_auto_retryable
        if self.missing is not None:
            debug_info["missing"] = self.missing
        if self.duplicated is not None:
            debug_info["duplicated"] = self.duplicated

        return debug_info


def _missing_or_duplicated_as_dict_list(items: Sequence[Any] | None) -> list[dict[str, Any]] | None:
    if items is None:
        return None
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            normalized.append(item)
        else:
            normalized.append({"identifier": item})
    return normalized or None


def toolkit_api_error_from_cognite(error: CogniteAPIError) -> ToolkitAPIError:
    """Build a ``ToolkitAPIError`` from a Cognite SDK ``CogniteAPIError`` (same surface fields where possible)."""
    return ToolkitAPIError(
        message=error.message,
        missing=_missing_or_duplicated_as_dict_list(error.missing),
        duplicated=_missing_or_duplicated_as_dict_list(error.duplicated),
        code=error.code,
        is_auto_retryable=None,
        request=None,
        response=None,
    )
