from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorModelRoutineFilter
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
                "create": Endpoint(method="POST", path="/simulators/routines", item_limit=1),
                "delete": Endpoint(method="POST", path="/simulators/routines/delete", item_limit=1),
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
        filter: SimulatorModelRoutineFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SimulatorRoutineResponse]:
        """Iterate over simulator routines in CDF.

        Args:
            filter: Filter to apply to the simulator routines.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SimulatorRoutineResponse objects.
        """
        return self._paginate(
            body={"filter": filter.dump() if filter else None},
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        filter: SimulatorModelRoutineFilter | None = None,
        limit: int = 100,
    ) -> Iterable[list[SimulatorRoutineResponse]]:
        """Iterate over simulator routines in CDF.

        Args:
            filter: Filter to apply to the simulator routines.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SimulatorRoutineResponse objects.
        """
        return self._iterate(
            limit=limit,
            body={
                "filter": filter.dump() if filter else None,
            },
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
