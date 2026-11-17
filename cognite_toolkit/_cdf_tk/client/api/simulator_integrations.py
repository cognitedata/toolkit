import builtins
from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorIntegrationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import SimulatorIntegrationResponse


class SimulatorIntegrationsAPI(CDFResourceAPI[SimulatorIntegrationResponse]):
    """The connectors that run simulations. Connectors register themselves, so they cannot be created here."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "delete": Endpoint(method="POST", path="/simulators/integrations/delete", item_limit=1),
                "list": Endpoint(method="POST", path="/simulators/integrations/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorIntegrationResponse]:
        return PagedResponse[SimulatorIntegrationResponse].model_validate_json(response.body)

    def delete(self, items: Sequence[InternalOrExternalId]) -> None:
        """Delete simulator integrations. Each integration is deleted with its own request.

        Args:
            items: The integrations to delete.
        """
        self._request_no_response(items, "delete")

    def list(
        self, filter: SimulatorIntegrationFilter | None = None, limit: int | None = None
    ) -> builtins.list[SimulatorIntegrationResponse]:
        """List simulator integrations. The endpoint returns at most 1000 integrations.

        Args:
            filter: Filter by simulator and whether the integration is active.
            limit: Maximum number of integrations to return.

        Returns:
            The integrations.
        """
        return self._list(limit=limit, body={"filter": filter.dump() if filter else None})
