"""Sessions API for creating and managing CDF sessions.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Sessions
"""

import builtins
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.session import (
    OneshotTokenExchangeSessionRequest,
    SessionCreateRequest,
    SessionCreateResponse,
    SessionResponse,
    SessionStatus,
)


class SessionAPI(CDFResourceAPI[SessionResponse]):
    """API for the Cognite Sessions endpoints.

    Sessions extend access to CDF resources. A session is created with client credentials,
    token exchange, or one-shot token exchange, then bound by the consumer using the returned nonce.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                # The create endpoint accepts exactly one item per request.
                "create": Endpoint(method="POST", path="/sessions", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/sessions/byids", item_limit=1000),
                "delete": Endpoint(method="POST", path="/sessions/revoke", item_limit=100),
                "list": Endpoint(method="GET", path="/sessions", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SessionResponse]:
        return PagedResponse[SessionResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SessionCreateRequest]) -> list[SessionCreateResponse]:
        """Create sessions.

        Each item is sent in its own request. The endpoint accepts exactly one session per call.

        Args:
            items: Session creation requests. Use client credentials, token exchange, or
                one-shot token exchange.

        Returns:
            Created sessions. Each item includes a nonce used to bind the session.
        """
        response_items: list[SessionCreateResponse] = []
        for response in self._chunk_requests(items, "create", self._serialize_items):
            response_items.extend(PagedResponse[SessionCreateResponse].model_validate_json(response.body).items)
        return response_items

    def create_one_shot_token_exchange_session(self) -> SessionCreateResponse:
        """Create a one-shot token exchange session.

        This is a convenience method for creating a single one-shot token exchange session
        without needing to construct a request object and index a list.

        Returns:
            Created session. The item includes a nonce used to bind the session.
        """
        response = self.create([OneshotTokenExchangeSessionRequest()])
        if not response:
            raise ToolkitAPIError("Failed to create one-shot token exchange session. No response received.")
        return response[0]

    def retrieve(self, items: Sequence[InternalId]) -> builtins.list[SessionResponse]:
        """Retrieve sessions by ID.

        The request fails if any ID does not belong to an existing session.

        Args:
            items: Session IDs to retrieve.

        Returns:
            Sessions for the given IDs.
        """
        return self._request_item_response(items, "retrieve")

    def revoke(self, items: Sequence[InternalId]) -> builtins.list[SessionResponse]:
        """Revoke sessions.

        Revocation is idempotent and may take up to one hour to take effect.
        When the caller lacks ``sessionsAcl:LIST``, each returned session contains only ``id``.

        Args:
            items: Session IDs to revoke.

        Returns:
            Revoked sessions.
        """
        return self._request_item_response(items, "delete")

    def paginate(
        self,
        status: SessionStatus | None = None,
        limit: int = 25,
        cursor: str | None = None,
    ) -> PagedResponse[SessionResponse]:
        """Fetch one page of sessions in the current project.

        Args:
            status: If given, only sessions with this status are returned.
            limit: Maximum number of sessions in the page. Maximum is 100. Default is 25.
            cursor: Cursor for pagination.

        Returns:
            One page of sessions.
        """
        return self._paginate(limit=limit, cursor=cursor, params=self._filter_out_none_values({"status": status}))

    def iterate(
        self,
        status: SessionStatus | None = None,
        limit: int | None = 25,
        cursor: str | None = None,
    ) -> Iterable[builtins.list[SessionResponse]]:
        """Iterate over sessions in the current project.

        Args:
            status: If given, only sessions with this status are returned.
            limit: Maximum number of sessions to return. Default is 25. ``None`` returns every session.
            cursor: Cursor to start pagination from.

        Returns:
            Batches of sessions.
        """
        return self._iterate(limit=limit, cursor=cursor, params=self._filter_out_none_values({"status": status}))

    def list(self, status: SessionStatus | None = None, limit: int | None = 25) -> builtins.list[SessionResponse]:
        """List sessions in the current project.

        Args:
            status: If given, only sessions with this status are returned.
            limit: Maximum number of sessions to return. Default is 25. ``None`` returns every session.

        Returns:
            Sessions in the current project.
        """
        return self._list(limit=limit, params=self._filter_out_none_values({"status": status}))
