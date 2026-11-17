import builtins
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.signal import SignalResponse, SignalSyncResponse, SignalTopic
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    CurrentUserSinkRef,
    SinkRefWithExternalId,
)


class SignalsAPI:
    """Read the signals delivered to sinks. Signals are created by CDF services, such as integrations and workflows."""

    _LIST_ENDPOINT = "/processes/signals/list"
    _SYNC_ENDPOINT = "/processes/signals/sync"
    _TOPICS_ENDPOINT = "/processes/signals/topics"
    _PAGE_LIMIT = 1000
    _API_VERSION = "alpha"

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _request(
        self, path: str, method: Literal["GET", "POST"], body: dict[str, Any] | None = None
    ) -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(path),
            method=method,
            body_content=body,
            api_version=self._API_VERSION,
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def list(
        self,
        sink: SinkRefWithExternalId | CurrentUserSinkRef,
        topic: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[SignalResponse]:
        """List the signals delivered to a sink.

        Args:
            sink: The sink the signals were delivered to.
            topic: Only return signals of this topic, for example "cognite_workflows".
            limit: Maximum number of signals to return. None returns all.

        Returns:
            The signals.
        """
        results: builtins.list[SignalResponse] = []
        cursor: str | None = None
        while True:
            page_limit = self._PAGE_LIMIT if limit is None else min(limit - len(results), self._PAGE_LIMIT)
            body: dict[str, Any] = {"sink": sink.dump(), "limit": page_limit}
            if topic is not None:
                body["topic"] = topic
            if cursor is not None:
                body["cursor"] = cursor
            page = PagedResponse[SignalResponse].model_validate_json(
                self._request(self._LIST_ENDPOINT, "POST", body).body
            )
            results.extend(page.items)
            cursor = page.next_cursor
            if cursor is None or not page.items or (limit is not None and len(results) >= limit):
                return results

    def sync(
        self, sink: SinkRefWithExternalId | CurrentUserSinkRef, cursor: str | None = None, limit: int = 100
    ) -> SignalSyncResponse:
        """Get the signals delivered to a sink since the previous sync.

        Args:
            sink: The sink the signals were delivered to.
            cursor: The cursor from the previous sync. Without it, the sync starts from the oldest signal.
            limit: Maximum number of signals to return, at most 1000.

        Returns:
            The signals and the cursor to pass to the next sync.
        """
        body: dict[str, Any] = {"sink": sink.dump(), "limit": limit}
        if cursor is not None:
            body["cursor"] = cursor
        return SignalSyncResponse.model_validate_json(self._request(self._SYNC_ENDPOINT, "POST", body).body)

    def topics(self) -> builtins.list[SignalTopic]:
        """List the topics signals can have, and the categories within each topic."""
        return ResponseItems[SignalTopic].model_validate_json(self._request(self._TOPICS_ENDPOINT, "GET").body).items
