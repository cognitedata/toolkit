import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest, LabelResponse


class LabelsAPI(CDFResourceAPI[LabelResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/labels", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/labels/byids", item_limit=1000),
                "delete": Endpoint(method="POST", path="/labels/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/labels/list", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[LabelResponse]:
        return PagedResponse[LabelResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[LabelRequest]) -> builtins.list[LabelResponse]:
        """Create labels in CDF.

        Args:
            items: List of LabelRequest objects to create.
        Returns:
            List of created LabelResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> builtins.list[LabelResponse]:
        """Retrieve labels from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved LabelResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete labels from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    @staticmethod
    def _list_body(filter: ClassicFilter | None, name: str | None) -> dict[str, Any] | None:
        dumped = filter.dump() if filter else {}
        if name is not None:
            dumped["name"] = name
        return {"filter": dumped} if dumped else None

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        name: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[LabelResponse]:
        """Get a page of labels from CDF.

        Args:
            filter: Filter by data set IDs and external ID prefix.
            name: Only return labels with exactly this name.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of LabelResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=self._list_body(filter, name))

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        name: str | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[LabelResponse]]:
        """Iterate over all labels in CDF.

        Args:
            filter: Filter by data set IDs and external ID prefix.
            name: Only return labels with exactly this name.
            limit: Maximum number of items to return.

        Returns:
            Iterable of lists of LabelResponse objects.
        """
        return self._iterate(limit=limit, body=self._list_body(filter, name))

    def list(
        self,
        filter: ClassicFilter | None = None,
        name: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[LabelResponse]:
        """List all labels in CDF.

        Args:
            filter: Filter by data set IDs and external ID prefix.
            name: Only return labels with exactly this name.
            limit: Maximum number of items to return.

        Returns:
            List of LabelResponse objects.
        """
        return self._list(limit=limit, body=self._list_body(filter, name))
