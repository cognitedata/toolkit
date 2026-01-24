"""Agents API for managing CDF AI agents.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/main_ai_agents_post

Note: This is an alpha API and may change in future releases.
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
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
                "upsert": Endpoint(method="POST", path="/ai/agents", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/ai/agents/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/ai/agents/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/ai/agents", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[AgentResponse]:
        return PagedResponse[AgentResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        """Apply (create or update) agents in CDF.

        Args:
            items: List of AgentRequest objects to apply.

        Returns:
            List of applied AgentResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        """Update agents in CDF.

        Args:
            items: List of AgentRequest objects to update.
        Returns:
            List of updated AgentResponse objects.
        """
        # Implemented as an alias to create (upsert) to have a standardized interface.
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[AgentResponse]:
        """Retrieve agents from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved AgentResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete agents from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

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
