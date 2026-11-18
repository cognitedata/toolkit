"""Data Product Roles API for managing roles of CDF data products."""

import builtins
from collections.abc import Iterable, Sequence
from functools import partial
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import DataProductRoleId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_role import (
    DataProductRoleRequest,
    DataProductRoleResponse,
)

from .data_product_role_access_filters import DataProductRoleAccessFiltersAPI


class DataProductRolesAPI(CDFResourceAPI[DataProductRoleResponse]):
    """API for managing data product roles.

    All endpoints are scoped under a parent data product: /dataproducts/{externalId}/roles.
    The API accepts one role per create, update, and delete request.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts/{externalId}/roles", item_limit=1),
                "update": Endpoint(method="POST", path="/dataproducts/{externalId}/roles/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/dataproducts/{externalId}/roles/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/dataproducts/{externalId}/roles", item_limit=100),
            },
            api_version="alpha",
        )
        self.access_filters = DataProductRoleAccessFiltersAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataProductRoleResponse]:
        return PagedResponse[DataProductRoleResponse].model_validate_json(response.body)

    def _path(self, method: Literal["create", "update", "delete", "list"], data_product_external_id: str) -> str:
        return self._method_endpoint_map[method].path.format(externalId=data_product_external_id)

    @staticmethod
    def _assign_data_product(items: Iterable[DataProductRoleResponse], data_product_external_id: str) -> None:
        for item in items:
            item.data_product_external_id = data_product_external_id

    def create(self, items: Sequence[DataProductRoleRequest]) -> builtins.list[DataProductRoleResponse]:
        """Create data product roles.

        Items are grouped by data_product_external_id and the path is formatted accordingly.

        Args:
            items: The roles to create.

        Returns:
            The created roles.
        """
        results: builtins.list[DataProductRoleResponse] = []
        for (data_product_external_id,), group in self._group_items_by_text_field(
            items, "data_product_external_id"
        ).items():
            created = self._request_item_response(
                group, "create", endpoint=self._path("create", data_product_external_id)
            )
            self._assign_data_product(created, data_product_external_id)
            results.extend(created)
        return results

    def update(
        self, items: Sequence[DataProductRoleRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[DataProductRoleResponse]:
        """Update data product roles.

        Items are grouped by data_product_external_id and the path is formatted accordingly.

        Args:
            items: The roles to update.
            mode: The update mode. "patch" only updates explicitly set fields, "replace" replaces all fields.

        Returns:
            The updated roles.
        """
        results: builtins.list[DataProductRoleResponse] = []
        for (data_product_external_id,), group in self._group_items_by_text_field(
            items, "data_product_external_id"
        ).items():
            for response in self._chunk_requests(
                group,
                "update",
                partial(self._serialize_updates, mode=mode),
                endpoint_path=self._path("update", data_product_external_id),
            ):
                updated = self._validate_page_response(response).items
                self._assign_data_product(updated, data_product_external_id)
                results.extend(updated)
        return results

    def delete(self, ids: Sequence[DataProductRoleId]) -> None:
        """Delete data product roles.

        Args:
            ids: The roles to delete.
        """
        for (data_product_external_id,), group in self._group_items_by_text_field(
            ids, "data_product_external_id"
        ).items():
            self._request_no_response(group, "delete", endpoint=self._path("delete", data_product_external_id))

    def paginate(
        self, data_product_external_id: str, limit: int = 10, cursor: str | None = None
    ) -> PagedResponse[DataProductRoleResponse]:
        page = self._paginate(limit=limit, cursor=cursor, endpoint_path=self._path("list", data_product_external_id))
        self._assign_data_product(page.items, data_product_external_id)
        return page

    def iterate(
        self, data_product_external_id: str, limit: int | None = 10
    ) -> Iterable[builtins.list[DataProductRoleResponse]]:
        for items in self._iterate(limit=limit, endpoint_path=self._path("list", data_product_external_id)):
            self._assign_data_product(items, data_product_external_id)
            yield items

    def list(self, data_product_external_id: str, limit: int | None = 10) -> builtins.list[DataProductRoleResponse]:
        items = self._list(limit=limit, endpoint_path=self._path("list", data_product_external_id))
        self._assign_data_product(items, data_product_external_id)
        return items
