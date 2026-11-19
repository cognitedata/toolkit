import builtins
from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import SimulatorLogResponse


class SimulatorLogsAPI(CDFResourceAPI[SimulatorLogResponse]):
    """Logs written by simulator connectors, for example for a simulation run or a model revision."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="POST", path="/simulators/logs/byids", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorLogResponse]:
        return PagedResponse[SimulatorLogResponse].model_validate_json(response.body)

    def retrieve(self, items: Sequence[InternalId]) -> builtins.list[SimulatorLogResponse]:
        """Retrieve simulator logs by ID, for example the ``log_id`` of a simulation run.

        Args:
            items: The logs to retrieve.

        Returns:
            The logs.
        """
        return self._request_item_response(items, "retrieve")
