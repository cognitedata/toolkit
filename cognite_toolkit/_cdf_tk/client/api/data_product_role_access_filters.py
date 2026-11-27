"""Data Product Role Access Filters API for managing the view access filters of data product roles."""

import builtins
from collections.abc import Iterable
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ViewNoVersionUntypedId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_role import (
    DataProductRoleAccessFilter,
    DataProductRoleAccessFilterRequest,
)


class DataProductRoleAccessFiltersAPI(CDFResourceAPI[DataProductRoleAccessFilter]):
    """API for managing the view access filters of data product roles.

    All endpoints are scoped under a parent data product: /dataproducts/{externalId}/roles/accessfilters.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "update": Endpoint(
                    method="POST", path="/dataproducts/{externalId}/roles/accessfilters/update", item_limit=1
                ),
                "list": Endpoint(method="GET", path="/dataproducts/{externalId}/roles/accessfilters", item_limit=100),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataProductRoleAccessFilter]:
        return PagedResponse[DataProductRoleAccessFilter].model_validate_json(response.body)

    def update(
        self,
        data_product_external_id: str,
        role_external_id: str,
        *,
        add: DataProductRoleAccessFilterRequest | None = None,
        update: DataProductRoleAccessFilterRequest | None = None,
        remove: ViewNoVersionUntypedId | None = None,
    ) -> builtins.list[DataProductRoleAccessFilter]:
        """Add, update, or remove the access filter of a role for one view each.

        Args:
            data_product_external_id: External ID of the data product the role belongs to.
            role_external_id: External ID of the role.
            add: A new access filter. Fails if the role already has an access filter for the view.
            update: Changes to an existing access filter. Fields that are not set are left unchanged,
                fields explicitly set to None are removed.
            remove: The view whose access filter is removed.

        Returns:
            The access filters of the role after the change.
        """
        body: dict[str, Any] = {"roleExternalId": role_external_id}
        if add is not None:
            body["add"] = [add.dump()]
        if update is not None:
            dumped = update.dump()
            patch: dict[str, Any] = {"view": dumped.pop("view")}
            for key, value in dumped.items():
                patch[key] = {"setNull": True} if value is None else {"set": value}
            body["update"] = [patch]
        if remove is not None:
            body["remove"] = [remove.dump(include_type=False)]
        if len(body) == 1:
            raise ValueError("At least one of add, update, or remove must be provided.")
        endpoint = self._method_endpoint_map["update"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(externalId=data_product_external_id)),
            method=endpoint.method,
            body_content=body,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items

    def paginate(
        self,
        data_product_external_id: str,
        role_external_id: str | None = None,
        limit: int = 10,
        cursor: str | None = None,
    ) -> PagedResponse[DataProductRoleAccessFilter]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        return self._paginate(
            limit=limit, cursor=cursor, params={"roleExternalId": role_external_id}, endpoint_path=path
        )

    def iterate(
        self, data_product_external_id: str, role_external_id: str | None = None, limit: int | None = 10
    ) -> Iterable[builtins.list[DataProductRoleAccessFilter]]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        return self._iterate(limit=limit, params={"roleExternalId": role_external_id}, endpoint_path=path)

    def list(
        self, data_product_external_id: str, role_external_id: str | None = None, limit: int | None = 10
    ) -> builtins.list[DataProductRoleAccessFilter]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        return self._list(limit=limit, params={"roleExternalId": role_external_id}, endpoint_path=path)
