from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorModelRoutineRevisionFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine_revision import (
    SimulatorRoutineRevisionRequest,
    SimulatorRoutineRevisionResponse,
)


class SimulatorRoutineRevisionsAPI(CDFResourceAPI[SimulatorRoutineRevisionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/simulators/routines/revisions", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/simulators/routines/revisions/byids", item_limit=1),
                "list": Endpoint(method="POST", path="/simulators/routines/revisions/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorRoutineRevisionResponse]:
        return PagedResponse[SimulatorRoutineRevisionResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SimulatorRoutineRevisionRequest]) -> list[SimulatorRoutineRevisionResponse]:
        """Create simulator routine revisions in CDF.

        Args:
            items: List of SimulatorRoutineRevisionRequest objects to create.

        Returns:
            List of created SimulatorRoutineRevisionResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[SimulatorRoutineRevisionResponse]:
        """Retrieve simulator routine revisions from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved SimulatorRoutineRevisionResponse objects.
        """
        if ignore_unknown_ids:
            return self._request_item_split_retries(items, method="retrieve")
        else:
            return self._request_item_response(items, method="retrieve")

    def paginate(
        self,
        filter: SimulatorModelRoutineRevisionFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SimulatorRoutineRevisionResponse]:
        """Iterate over simulator routine revisions in CDF.

        Args:
            filter: Filter to apply to the list of simulator routine revisions.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SimulatorRoutineRevisionResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={
                "filter": filter.dump() if filter else None,
            },
        )

    def iterate(
        self,
        filter: SimulatorModelRoutineRevisionFilter | None = None,
        limit: int = 100,
    ) -> Iterable[list[SimulatorRoutineRevisionResponse]]:
        """Iterate over simulator routine revisions in CDF.

        Args:
            filter: Filter to apply to the list of simulator routine revisions.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SimulatorRoutineRevisionResponse objects.
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
    ) -> list[SimulatorRoutineRevisionResponse]:
        """List all simulator routine revisions in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of SimulatorRoutineRevisionResponse objects.
        """
        return self._list(limit=limit)
