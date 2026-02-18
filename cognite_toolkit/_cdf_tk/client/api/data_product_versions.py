"""Data Product Versions API for managing versions of CDF data products."""

from __future__ import annotations

import builtins
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionRequest,
    DataProductVersionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import DataProductVersionId


class DataProductVersionsAPI(
    CDFResourceAPI[DataProductVersionId, DataProductVersionRequest, DataProductVersionResponse]
):
    """API for managing data product versions.

    All endpoints are scoped under a parent data product: /dataproducts/{externalId}/versions.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts/{externalId}/versions", item_limit=1),
                "retrieve": Endpoint(
                    method="GET", path="/dataproducts/{externalId}/versions/{versionId}", item_limit=1
                ),
                "update": Endpoint(method="POST", path="/dataproducts/{externalId}/versions/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/dataproducts/{externalId}/versions/delete", item_limit=10),
                "list": Endpoint(method="GET", path="/dataproducts/{externalId}/versions", item_limit=10),
            },
            api_version="alpha",
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataProductVersionResponse]:
        return PagedResponse[DataProductVersionResponse].model_validate_json(response.body)

    def create(
        self, data_product_external_id: str, items: Sequence[DataProductVersionRequest]
    ) -> builtins.list[DataProductVersionResponse]:
        endpoint = self._method_endpoint_map["create"]
        path = endpoint.path.format(externalId=data_product_external_id)
        results: builtins.list[DataProductVersionResponse] = []
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(path),
                    method=endpoint.method,
                    body_content={"items": [item.dump()]},
                    api_version=self._api_version,
                )
            )
            page = self._validate_page_response(response.get_success_or_raise())
            for version in page.items:
                version.data_product_external_id = data_product_external_id
            results.extend(page.items)
        return results

    def retrieve_by_version_id(
        self,
        data_product_external_id: str,
        version_id: int,
        ignore_unknown_ids: bool = False,
    ) -> DataProductVersionResponse | None:
        endpoint = self._method_endpoint_map["retrieve"]
        path = endpoint.path.format(externalId=data_product_external_id, versionId=version_id)
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(path),
                method=endpoint.method,
                api_version=self._api_version,
            )
        )
        if isinstance(response, SuccessResponse):
            version = DataProductVersionResponse.model_validate_json(response.body)
            version.data_product_external_id = data_product_external_id
            return version
        if ignore_unknown_ids:
            return None
        _ = response.get_success_or_raise()
        return None

    def update(
        self,
        data_product_external_id: str,
        version_id: int,
        item: DataProductVersionRequest,
    ) -> DataProductVersionResponse:
        endpoint = self._method_endpoint_map["update"]
        path = endpoint.path.format(externalId=data_product_external_id)

        update_body: dict = {"versionId": version_id, "update": {}}
        if item.status is not None:
            update_body["update"]["status"] = {"set": item.status}
        if item.description is not None:
            update_body["update"]["description"] = {"set": item.description}
        if item.terms is not None:
            terms_modify: dict = {}
            if item.terms.usage is not None:
                terms_modify["usage"] = {"set": item.terms.usage}
            if item.terms.limitations is not None:
                terms_modify["limitations"] = {"set": item.terms.limitations}
            if terms_modify:
                update_body["update"]["terms"] = {"modify": terms_modify}
        if item.data_model and item.data_model.views is not None:
            update_body["update"]["dataModel"] = {
                "modify": {"views": {"set": [v.dump() for v in item.data_model.views]}}
            }

        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(path),
                method=endpoint.method,
                body_content={"items": [update_body]},
                api_version=self._api_version,
            )
        )
        page = self._validate_page_response(response.get_success_or_raise())
        for version in page.items:
            version.data_product_external_id = data_product_external_id
        return page.items[0]

    def delete(self, data_product_external_id: str, version_ids: Sequence[int]) -> None:
        endpoint = self._method_endpoint_map["delete"]
        path = endpoint.path.format(externalId=data_product_external_id)
        self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(path),
                method=endpoint.method,
                body_content={"items": [{"versionId": vid} for vid in version_ids]},
                api_version=self._api_version,
            )
        ).get_success_or_raise()

    def list(self, data_product_external_id: str, limit: int | None = 10) -> builtins.list[DataProductVersionResponse]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(externalId=data_product_external_id)
        all_items: builtins.list[DataProductVersionResponse] = []
        cursor: str | None = None
        remaining = limit

        while True:
            page_limit = min(remaining, 10) if remaining is not None else 10
            params: dict = {"limit": page_limit}
            if cursor:
                params["cursor"] = cursor

            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(path),
                    method=endpoint.method,
                    parameters=params,
                    api_version=self._api_version,
                )
            )
            page = self._validate_page_response(response.get_success_or_raise())
            for version in page.items:
                version.data_product_external_id = data_product_external_id
            all_items.extend(page.items)

            if remaining is not None:
                remaining -= len(page.items)
                if remaining <= 0:
                    break
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        return all_items

    def iterate(
        self, data_product_external_id: str, limit: int | None = 10
    ) -> Iterable[builtins.list[DataProductVersionResponse]]:
        endpoint = self._method_endpoint_map["list"]
        path = endpoint.path.format(externalId=data_product_external_id)
        cursor: str | None = None
        remaining = limit

        while True:
            page_limit = min(remaining, 10) if remaining is not None else 10
            params: dict = {"limit": page_limit}
            if cursor:
                params["cursor"] = cursor

            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(path),
                    method=endpoint.method,
                    parameters=params,
                    api_version=self._api_version,
                )
            )
            page = self._validate_page_response(response.get_success_or_raise())
            for version in page.items:
                version.data_product_external_id = data_product_external_id

            if page.items:
                yield page.items

            if remaining is not None:
                remaining -= len(page.items)
                if remaining <= 0:
                    break
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
