from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2


class AssetsAPI(CDFResourceAPI[InternalOrExternalId, AssetRequest, AssetResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/assets", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/assets/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/assets/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/assets/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/assets/list", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2 | ItemsSuccessResponse2) -> PagedResponse[AssetResponse]:
        return PagedResponse[AssetResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[AssetRequest]) -> list[AssetResponse]:
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
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved AssetResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[AssetRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[AssetResponse]:
        """Update assets in CDF.

        Args:
            items: List of AssetRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated AssetResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(
        self, items: Sequence[InternalOrExternalId], recursive: bool = False, ignore_unknown_ids: bool = False
    ) -> None:
        """Delete assets from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            recursive: Whether to delete assets recursively.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(
            items, "delete", extra_body={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids}
        )

    def iterate(
        self,
        aggregated_properties: bool = False,
        data_set_external_ids: list[str] | None = None,
        asset_subtree_external_ids: list[str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[AssetResponse]:
        """Iterate over all assets in CDF.

        Returns:
            PagedResponse of AssetResponse objects.
        """
        filter_: dict[str, Any] = {}
        if asset_subtree_external_ids:
            filter_["assetSubtreeExternalIds"] = [{"externalId": ext_id} for ext_id in asset_subtree_external_ids]
        if data_set_external_ids:
            filter_["dataSetIds"] = [{"externalId": ds_id} for ds_id in data_set_external_ids]

        return self._iterate(
            cursor=cursor,
            limit=limit,
            body={
                "aggregatedProperties": ["childCount", "path", "depth"] if aggregated_properties else [],
                "filter": filter_ or None,
            },
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[AssetResponse]:
        """List all asset references in CDF.

        Returns:
            ResponseItems of T_Identifier objects.
        """
        return self._list(limit=limit)
