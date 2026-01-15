"""Agents API for managing CDF AI agents.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/main_ai_agents_post

Note: This is an alpha API and may change in future releases.
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class AgentsAPI(CDFResourceAPI[ExternalId, AgentRequest, AgentResponse]):
    """API for managing CDF AI agents.

    Note: This is an alpha API and may change in future releases.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/ai/agents", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/ai/agents/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/ai/agents/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/ai/agents", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[AgentResponse]:
        return PagedResponse[AgentResponse].model_validate_json(response.body)

    def apply(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        """Apply (create or update) agents in CDF.

        Args:
            items: List of AgentRequest objects to apply.

        Returns:
            List of applied AgentResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ExternalId]) -> list[AgentResponse]:
        """Retrieve agents from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.

        Returns:
            List of retrieved AgentResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete agents from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AgentResponse]:
        """Get a page of agents from CDF.

        Args:
            limit: Maximum number of agents to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of AgentResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[list[AgentResponse]]:
        """Iterate over all agents in CDF.

        Args:
            limit: Maximum total number of agents to return.

        Returns:
            Iterable of lists of AgentResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[AgentResponse]:
        """List all agents in CDF.

        Args:
            limit: Maximum total number of agents to return.

        Returns:
            List of AgentResponse objects.
        """
        return self._list(limit=limit)
