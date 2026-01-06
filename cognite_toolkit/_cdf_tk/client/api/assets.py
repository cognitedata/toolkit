from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


class AssetsAPI(CDFResourceAPI[InternalOrExternalId, AssetRequest, AssetResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            resource_endpoint="/assets",
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/assets", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/assets/retrieve", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(method="POST", path="/assets/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="GET", path="/assets", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[AssetResponse]:
        return PagedResponse[AssetResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: list[AssetRequest]) -> list[AssetResponse]:
        """Create assets in CDF.

        Args:
            items: List of AssetRequest objects to create.
        Returns:
            List of created AssetResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> list[AssetResponse]:
        """Retrieve assets from CDF.

        Args:
            items: List of AssetRequest objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved AssetResponse objects.
        """
        return self._request_item_response(items, method="retrieve", params={"ignoreUnknownIds": ignore_unknown_ids})

    def update(self, items: list[AssetRequest]) -> list[AssetResponse]:
        """Update assets in CDF.

        Args:
            items: List of AssetRequest objects to update.
        Returns:
            List of updated AssetResponse objects.
        """
        raise NotImplementedError()

    def delete(
        self, items: list[InternalOrExternalId], recursive: bool = False, ignore_unknown_ids: bool = False
    ) -> None:
        """Delete assets from CDF.

        Args:
            items: List of AssetRequest objects to delete.
            recursive: Whether to delete assets recursively.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(
            items, "delete", params={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids}
        )

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AssetResponse]:
        """Iterate over all assets in CDF.

        Returns:
            PagedResponse of AssetResponse objects.
        """
        return self._iterate(cursor=cursor, limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[AssetResponse]:
        """List all asset references in CDF.

        Returns:
            ResponseItems of T_Identifier objects.
        """
        return self._list(limit=limit)
