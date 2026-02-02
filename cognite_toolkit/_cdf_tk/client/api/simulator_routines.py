from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine import (
    SimulatorRoutineRequest,
    SimulatorRoutineResponse,
)


class SimulatorRoutinesAPI(CDFResourceAPI[InternalOrExternalId, SimulatorRoutineRequest, SimulatorRoutineResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="POST", path="/simulators/routines", item_limit=1, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/simulators/routines/delete", item_limit=1, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path="/simulators/routines/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorRoutineResponse]:
        return PagedResponse[SimulatorRoutineResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SimulatorRoutineRequest]) -> list[SimulatorRoutineResponse]:
        """Create simulator routines in CDF.

        Args:
            items: List of SimulatorRoutineRequest objects to create.

        Returns:
            List of created SimulatorRoutineResponse objects.
        """
        return self._request_item_response(items, "create")

    def delete(self, items: Sequence[InternalOrExternalId]) -> None:
        """Delete simulator routines from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        model_external_ids: list[str] | None = None,
        simulator_integration_external_ids: list[str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SimulatorRoutineResponse]:
        """Iterate over simulator routines in CDF.

        Args:
            model_external_ids: Filter by model external IDs.
            simulator_integration_external_ids: Filter by simulator integration external IDs.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SimulatorRoutineResponse objects.
        """
        filter_: dict[str, Any] = {}
        if model_external_ids:
            filter_["modelExternalIds"] = model_external_ids
        if simulator_integration_external_ids:
            filter_["simulatorIntegrationExternalIds"] = simulator_integration_external_ids

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_ or None},
        )

    def iterate(
        self,
        model_external_ids: list[str] | None = None,
        simulator_integration_external_ids: list[str] | None = None,
        limit: int = 100,
    ) -> Iterable[list[SimulatorRoutineResponse]]:
        """Iterate over simulator routines in CDF.

        Args:
            model_external_ids: Filter by model external IDs.
            simulator_integration_external_ids: Filter by simulator integration external IDs.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SimulatorRoutineResponse objects.
        """
        filter_: dict[str, Any] = {}
        if model_external_ids:
            filter_["modelExternalIds"] = model_external_ids
        if simulator_integration_external_ids:
            filter_["simulatorIntegrationExternalIds"] = simulator_integration_external_ids

        return self._iterate(
            limit=limit,
            body={"filter": filter_ or None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[SimulatorRoutineResponse]:
        """List all simulator routines in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of SimulatorRoutineResponse objects.
        """
        return self._list(limit=limit)
