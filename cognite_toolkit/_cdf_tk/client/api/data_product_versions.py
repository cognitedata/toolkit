"""Data Product Versions API for managing versions of CDF data products."""

from __future__ import annotations

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
    Versions are identified by a user-specified semantic version string (major.minor.patch).
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts/{externalId}/versions", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/dataproducts/{externalId}/versions/{version}", item_limit=1),
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
    ) -> list[DataProductVersionResponse]:
        endpoint = self._method_endpoint_map["create"]
        path = endpoint.path.format(externalId=data_product_external_id)
        results: list[DataProductVersionResponse] = []
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

    def retrieve(
        self,
        data_product_external_id: str,
        version: str,
        ignore_unknown_ids: bool = False,
    ) -> DataProductVersionResponse | None:
        endpoint = self._method_endpoint_map["retrieve"]
        path = endpoint.path.format(externalId=data_product_external_id, version=version)
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(path),
                method=endpoint.method,
                api_version=self._api_version,
            )
        )
        if isinstance(response, SuccessResponse):
            ver = DataProductVersionResponse.model_validate_json(response.body)
            ver.data_product_external_id = data_product_external_id
            return ver
        if ignore_unknown_ids:
            return None
        _ = response.get_success_or_raise()
        return None

    def update(
        self,
        data_product_external_id: str,
        version: str,
        item: DataProductVersionRequest,
    ) -> DataProductVersionResponse:
        endpoint = self._method_endpoint_map["update"]
        path = endpoint.path.format(externalId=data_product_external_id)

        update_body: dict = {"version": version, "update": {}}
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
        for ver in page.items:
            ver.data_product_external_id = data_product_external_id
        return page.items[0]

    def delete(self, data_product_external_id: str, versions: Sequence[str]) -> None:
        endpoint = self._method_endpoint_map["delete"]
        path = endpoint.path.format(externalId=data_product_external_id)
        self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(path),
                method=endpoint.method,
                body_content={"items": [{"version": v} for v in versions]},
                api_version=self._api_version,
            )
        ).get_success_or_raise()

    def list(self, data_product_external_id: str, limit: int | None = 10) -> list[DataProductVersionResponse]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        items = self._list(limit=limit, endpoint_path=path)
        for item in items:
            item.data_product_external_id = data_product_external_id
        return items

    def iterate(
        self, data_product_external_id: str, limit: int | None = 10
    ) -> Iterable[list[DataProductVersionResponse]]:  # type: ignore[valid-type]
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        for batch in self._iterate(limit=limit, endpoint_path=path):
            for item in batch:
                item.data_product_external_id = data_product_external_id
            yield batch
