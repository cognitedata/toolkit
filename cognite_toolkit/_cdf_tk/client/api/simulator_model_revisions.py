from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import SimulatorModelRevisionFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model_revision import (
    SimulatorModelRevisionRequest,
    SimulatorModelRevisionResponse,
)


class SimulatorModelRevisionsAPI(
    CDFResourceAPI[InternalOrExternalId, SimulatorModelRevisionRequest, SimulatorModelRevisionResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/simulators/models/revisions", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/simulators/models/revisions/byids", item_limit=1),
                "list": Endpoint(method="POST", path="/simulators/models/revisions/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorModelRevisionResponse]:
        return PagedResponse[SimulatorModelRevisionResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SimulatorModelRevisionRequest]) -> list[SimulatorModelRevisionResponse]:
        """Create simulator model revisions in CDF.

        Args:
            items: List of SimulatorModelRevisionRequest objects to create.

        Returns:
            List of created SimulatorModelRevisionResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[SimulatorModelRevisionResponse]:
        """Retrieve simulator model revisions from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved SimulatorModelRevisionResponse objects.
        """
        if ignore_unknown_ids:
            return self._request_item_split_retries(items, method="retrieve")
        else:
            return self._request_item_response(items, method="retrieve")

    def paginate(
        self,
        filter: SimulatorModelRevisionFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SimulatorModelRevisionResponse]:
        """Iterate over simulator model revisions in CDF.

        Args:
            filter: Filter to apply to the simulator model revisions.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SimulatorModelRevisionResponse objects.
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
        filter: SimulatorModelRevisionFilter | None = None,
        limit: int = 100,
    ) -> Iterable[list[SimulatorModelRevisionResponse]]:
        """Iterate over simulator model revisions in CDF.

        Args:
            filter: Filter to apply to the simulator model revisions.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SimulatorModelRevisionResponse objects.
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
    ) -> list[SimulatorModelRevisionResponse]:
        """List all simulator model revisions in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of SimulatorModelRevisionResponse objects.
        """
        return self._list(limit=limit)
