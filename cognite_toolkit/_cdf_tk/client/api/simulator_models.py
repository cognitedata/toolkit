from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)


class SimulatorModelsAPI(CDFResourceAPI[InternalOrExternalId, SimulatorModelRequest, SimulatorModelResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/simulators/models", item_limit=1, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/simulators/models/byids", item_limit=1, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/simulators/models/update", item_limit=1, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/simulators/models/delete", item_limit=1, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path="/simulators/models/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SimulatorModelResponse]:
        return PagedResponse[SimulatorModelResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[SimulatorModelRequest]) -> list[SimulatorModelResponse]:
        """Create simulator models in CDF.

        Args:
            items: List of SimulatorModelRequest objects to create.

        Returns:
            List of created SimulatorModelResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[SimulatorModelResponse]:
        """Retrieve simulator models from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved SimulatorModelResponse objects.
        """
        if ignore_unknown_ids:
            # The CDF API does not support ignore_unknown_ids for simulator models,
            # so we implement it with retries here.
            return self._request_item_split_retries(items, method="retrieve")
        else:
            return self._request_item_response(items, method="retrieve")

    def update(
        self, items: Sequence[SimulatorModelRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[SimulatorModelResponse]:
        """Update simulator models in CDF.

        Args:
            items: List of SimulatorModelRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated SimulatorModelResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId]) -> None:
        """Delete simulator models from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        simulator_external_ids: list[str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[SimulatorModelResponse]:
        """Iterate over simulator models in CDF.

        Args:
            simulator_external_ids: Filter by simulator external IDs.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of SimulatorModelResponse objects.
        """
        filter_: dict[str, Any] = {}
        if simulator_external_ids:
            filter_["simulatorExternalIds"] = simulator_external_ids

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_ or None},
        )

    def iterate(
        self,
        simulator_external_ids: list[str] | None = None,
        limit: int = 100,
    ) -> Iterable[list[SimulatorModelResponse]]:
        """Iterate over simulator models in CDF.

        Args:
            simulator_external_ids: Filter by simulator external IDs.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of SimulatorModelResponse objects.
        """
        filter_: dict[str, Any] = {}
        if simulator_external_ids:
            filter_["simulatorExternalIds"] = simulator_external_ids

        return self._iterate(
            limit=limit,
            body={"filter": filter_ or None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[SimulatorModelResponse]:
        """List all simulator models in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of SimulatorModelResponse objects.
        """
        return self._list(limit=limit)
