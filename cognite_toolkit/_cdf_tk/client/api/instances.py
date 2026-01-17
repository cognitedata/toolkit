from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import InstanceRequest, InstanceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import TypedInstanceIdentifier


class LabelsAPI(CDFResourceAPI[TypedInstanceIdentifier, InstanceRequest, InstanceResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/instances", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/models/instances/byids", item_limit=1000),
                "delete": Endpoint(method="POST", path="/models/instances/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/models/instances/query", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[InstanceResponse]:
        return PagedResponse[InstanceResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[TypedInstanceIdentifier]:
        return ResponseItems[TypedInstanceIdentifier].model_validate_json(response.body)

    def create(self, items: Sequence[InstanceRequest]) -> list[InstanceResponse]:
        """Create instances in CDF.

        Args:
            items: List of LabelRequest objects to create.
        Returns:
            List of created LabelResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[TypedInstanceIdentifier]) -> list[InstanceResponse]:
        """Retrieve instances from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
        Returns:
            List of retrieved LabelResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[TypedInstanceIdentifier]) -> None:
        """Delete instances from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        name: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[InstanceResponse]:
        """Iterate over all instances in CDF.

        Args:
            filter: Filter by data set IDs.
            name: Filter by instance name (prefix match).
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of LabelResponse objects.
        """
        body: dict[str, Any] = filter.dump() if filter else {}
        if name:
            body["name"] = name

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=body,
        )

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        name: str | None = None,
        limit: int = 100,
    ) -> Iterable[list[InstanceResponse]]:
        """Iterate over all instances in CDF.

        Args:
            filter: Filter by data set IDs.
            name: Filter by instance name (prefix match).
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of LabelResponse objects.
        """
        body: dict[str, Any] = filter.dump() if filter else {}
        if name:
            body["name"] = name

        return self._iterate(
            limit=limit,
            body=body,
        )

    def list(self, limit: int | None = 100) -> list[InstanceResponse]:
        """List all instances in CDF.

        Returns:
            List of LabelResponse objects.
        """
        return self._list(limit=limit)
