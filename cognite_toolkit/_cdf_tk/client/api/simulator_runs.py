import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulationRunFilter, SimulationRunSort
from cognite_toolkit._cdf_tk.client.resource_classes.simulator import (
    SimulationRunDataResponse,
    SimulationRunRequest,
    SimulationRunResponse,
)


class SimulatorRunsAPI(CDFResourceAPI[SimulationRunResponse]):
    """Run simulations and read their status, inputs, and outputs."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/simulators/run", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/simulators/runs/byids", item_limit=1),
                "list": Endpoint(method="POST", path="/simulators/runs/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulationRunResponse]:
        return PagedResponse[SimulationRunResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SimulationRunRequest]) -> builtins.list[SimulationRunResponse]:
        """Start simulation runs. Each run is started with its own request.

        Args:
            items: The runs to start.

        Returns:
            The started runs. Retrieve them later to get their status.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId]) -> builtins.list[SimulationRunResponse]:
        """Retrieve simulation runs by ID. Each run is retrieved with its own request.

        Args:
            items: The runs to retrieve.

        Returns:
            The runs.
        """
        return self._request_item_response(items, "retrieve")

    def list_data(self, run_ids: Sequence[int]) -> builtins.list[SimulationRunDataResponse]:
        """Retrieve the input and output values of simulation runs. Each run is retrieved with its own request.

        Args:
            run_ids: The IDs of the runs.

        Returns:
            The inputs and outputs of each run.
        """
        results: builtins.list[SimulationRunDataResponse] = []
        for run_id in run_ids:
            request = RequestMessage(
                endpoint_url=self._make_url("/simulators/runs/data/list"),
                method="POST",
                body_content={"items": [{"runId": run_id}]},
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            results.extend(ResponseItems[SimulationRunDataResponse].model_validate_json(response.body).items)
        return results

    def paginate(
        self,
        filter: SimulationRunFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
        sort: SimulationRunSort | None = None,
    ) -> PagedResponse[SimulationRunResponse]:
        """Fetch a page of simulation runs.

        Args:
            filter: Filter by status, run type, simulator, model, routine, and time.
            limit: Maximum number of runs to return.
            cursor: Cursor for pagination.
            sort: Sort by created time or simulation time.

        Returns:
            A page of runs.
        """
        return self._paginate(limit=limit, cursor=cursor, body=self._list_body(filter, sort))

    def iterate(
        self,
        filter: SimulationRunFilter | None = None,
        limit: int | None = 100,
        sort: SimulationRunSort | None = None,
    ) -> Iterable[builtins.list[SimulationRunResponse]]:
        """Iterate over simulation runs.

        Args:
            filter: Filter by status, run type, simulator, model, routine, and time.
            limit: Maximum number of runs to return. None returns all.
            sort: Sort by created time or simulation time.

        Returns:
            Pages of runs.
        """
        return self._iterate(limit=limit, body=self._list_body(filter, sort))

    def list(
        self,
        filter: SimulationRunFilter | None = None,
        limit: int | None = 100,
        sort: SimulationRunSort | None = None,
    ) -> builtins.list[SimulationRunResponse]:
        """List simulation runs.

        Args:
            filter: Filter by status, run type, simulator, model, routine, and time.
            limit: Maximum number of runs to return. None returns all.
            sort: Sort by created time or simulation time.

        Returns:
            The runs.
        """
        return self._list(limit=limit, body=self._list_body(filter, sort))

    @staticmethod
    def _list_body(filter: SimulationRunFilter | None, sort: SimulationRunSort | None) -> dict[str, Any]:
        # The API accepts a list but only supports sorting by one property.
        return {"filter": filter.dump() if filter else None, "sort": [sort.dump()] if sort else None}
