import builtins
from collections.abc import Iterable
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.signal import (
    SignalResponse,
    SignalSyncResponse,
    SignalTopic,
    SignalTopicName,
)
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    CurrentUserSinkRef,
    SinkRefWithExternalId,
)


class SignalsAPI(CDFResourceAPI[SignalResponse]):
    """Read the signals delivered to sinks. Signals are created by CDF services, such as integrations and workflows."""

    _SYNC_ENDPOINT = "/processes/signals/sync"
    _TOPICS_ENDPOINT = "/processes/signals/topics"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "list": Endpoint(method="POST", path="/processes/signals/list", item_limit=1000),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SignalResponse]:
        return PagedResponse[SignalResponse].model_validate_json(response.body)

    def _request(
        self, path: str, method: Literal["GET", "POST"], body: dict[str, Any] | None = None
    ) -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._make_url(path),
            method=method,
            body_content=body,
            disable_gzip=self._disable_gzip,
            api_version=self._api_version,
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    @staticmethod
    def _create_list_body(
        sink: SinkRefWithExternalId | CurrentUserSinkRef, topic: SignalTopicName | None
    ) -> dict[str, Any]:
        return {"sink": sink.dump(), "topic": topic}

    def paginate(
        self,
        sink: SinkRefWithExternalId | CurrentUserSinkRef,
        topic: SignalTopicName | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SignalResponse]:
        """Fetch a page of the signals delivered to a sink.

        Args:
            sink: The sink the signals were delivered to.
            topic: Only return signals of this topic.
            limit: Maximum number of signals to return, at most 1000.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of signals.
        """
        return self._paginate(limit=limit, cursor=cursor, body=self._create_list_body(sink, topic))

    def iterate(
        self,
        sink: SinkRefWithExternalId | CurrentUserSinkRef,
        topic: SignalTopicName | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[SignalResponse]]:
        """Iterate over the signals delivered to a sink.

        Args:
            sink: The sink the signals were delivered to.
            topic: Only return signals of this topic.
            limit: Maximum number of signals to return. None returns all.

        Returns:
            Iterable of lists of signals.
        """
        return self._iterate(limit=limit, body=self._create_list_body(sink, topic))

    def list(
        self,
        sink: SinkRefWithExternalId | CurrentUserSinkRef,
        topic: SignalTopicName | None = None,
        limit: int | None = 100,
    ) -> builtins.list[SignalResponse]:
        """List the signals delivered to a sink.

        Args:
            sink: The sink the signals were delivered to.
            topic: Only return signals of this topic.
            limit: Maximum number of signals to return. None returns all.

        Returns:
            The signals.
        """
        return self._list(limit=limit, body=self._create_list_body(sink, topic))

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
