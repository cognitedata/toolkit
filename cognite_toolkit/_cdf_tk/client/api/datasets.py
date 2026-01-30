from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId


class DataSetsAPI(CDFResourceAPI[InternalOrExternalId, DataSetRequest, DataSetResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/datasets", item_limit=10, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/datasets/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/datasets/update", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/datasets/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataSetResponse]:
        return PagedResponse[DataSetResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[DataSetRequest]) -> list[DataSetResponse]:
        """Create data sets in CDF.

        Args:
            items: List of DataSetRequest objects to create.
        Returns:
            List of created DataSetResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[DataSetResponse]:
        """Retrieve data sets from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved DataSetResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[DataSetRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[DataSetResponse]:
        """Update data sets in CDF.

        Args:
            items: List of DataSetRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated DataSetResponse objects.
        """
        return self._update(items, mode=mode)

    def paginate(
        self,
        metadata: dict[str, str] | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[DataSetResponse]:
        """Iterate over all data sets in CDF.

        Args:
            metadata: Filter by metadata.
            external_id_prefix: Filter by external ID prefix.
            write_protected: Filter by write protection status.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of DataSetResponse objects.
        """
        filter_body: dict[str, Any] = {}
        if metadata is not None:
            filter_body["metadata"] = metadata
        if external_id_prefix is not None:
            filter_body["externalIdPrefix"] = external_id_prefix
        if write_protected is not None:
            filter_body["writeProtected"] = write_protected

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_body} if filter_body else {},
        )

    def iterate(
        self,
        metadata: dict[str, str] | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int = 100,
    ) -> Iterable[list[DataSetResponse]]:
        """Iterate over all data sets in CDF.

        Args:
            metadata: Filter by metadata.
            external_id_prefix: Filter by external ID prefix.
            write_protected: Filter by write protection status.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of DataSetResponse objects.
        """
        filter_body: dict[str, Any] = {}
        if metadata is not None:
            filter_body["metadata"] = metadata
        if external_id_prefix is not None:
            filter_body["externalIdPrefix"] = external_id_prefix
        if write_protected is not None:
            filter_body["writeProtected"] = write_protected

        return self._iterate(
            limit=limit,
            body={"filter": filter_body} if filter_body else {},
        )

    def list(self, limit: int | None = 100) -> list[DataSetResponse]:
        """List all data sets in CDF.

        Returns:
            List of DataSetResponse objects.
        """
        return self._list(limit=limit)
