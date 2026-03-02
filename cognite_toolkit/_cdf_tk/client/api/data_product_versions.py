"""Data Product Versions API for managing versions of CDF data products."""

from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import DataProductVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionRequest,
    DataProductVersionResponse,
)


class DataProductVersionsAPI(CDFResourceAPI[DataProductVersionResponse]):
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

    @staticmethod
    def _group_by_parent(
        items: Sequence[DataProductVersionRequest],
    ) -> dict[str, list[DataProductVersionRequest]]:
        by_parent: dict[str, list[DataProductVersionRequest]] = {}
        for item in items:
            by_parent.setdefault(item.data_product_external_id, []).append(item)
        return by_parent

    def create(self, items: Sequence[DataProductVersionRequest]) -> list[DataProductVersionResponse]:
        results: list[DataProductVersionResponse] = []
        for dp_ext_id, group in self._group_by_parent(items).items():
            url = self._make_url(self._method_endpoint_map["create"].path.format(externalId=dp_ext_id))
            for item in group:
                response = self._http_client.request_single_retries(
                    RequestMessage(
                        endpoint_url=url,
                        method="POST",
                        body_content={"items": [item.dump()]},
                        api_version=self._api_version,
                    )
                )
                page = self._validate_page_response(response.get_success_or_raise())
                for version in page.items:
                    version.data_product_external_id = dp_ext_id
                results.extend(page.items)
        return results

    def retrieve(
        self,
        items: Sequence[DataProductVersionId],
        ignore_unknown_ids: bool = False,
    ) -> list[DataProductVersionResponse]:
        results: list[DataProductVersionResponse] = []
        for item in items:
            url = self._make_url(
                self._method_endpoint_map["retrieve"].path.format(
                    externalId=item.data_product_external_id, version=item.version
                )
            )
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=url,
                    method="GET",
                    api_version=self._api_version,
                )
            )
            if isinstance(response, SuccessResponse):
                ver = DataProductVersionResponse.model_validate_json(response.body)
                ver.data_product_external_id = item.data_product_external_id
                results.append(ver)
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise()
        return results

    def update(
        self,
        items: Sequence[DataProductVersionRequest],
        mode: Literal["patch", "replace"] = "replace",
    ) -> list[DataProductVersionResponse]:
        results: list[DataProductVersionResponse] = []
        for dp_ext_id, group in self._group_by_parent(items).items():
            url = self._make_url(self._method_endpoint_map["update"].path.format(externalId=dp_ext_id))
            for item in group:
                response = self._http_client.request_single_retries(
                    RequestMessage(
                        endpoint_url=url,
                        method="POST",
                        body_content={"items": [item.as_update(mode=mode)]},
                        api_version=self._api_version,
                    )
                )
                page = self._validate_page_response(response.get_success_or_raise())
                for ver in page.items:
                    ver.data_product_external_id = dp_ext_id
                results.extend(page.items)
        return results

    def delete(self, ids: Sequence[DataProductVersionId]) -> None:
        by_parent: defaultdict[str, list[str]] = defaultdict(list)
        for id_ in ids:
            by_parent[id_.data_product_external_id].append(id_.version)
        for dp_ext_id, versions in by_parent.items():
            url = self._make_url(self._method_endpoint_map["delete"].path.format(externalId=dp_ext_id))
            self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=url,
                    method="POST",
                    body_content={"items": [{"version": v} for v in versions]},
                    api_version=self._api_version,
                )
            ).get_success_or_raise()

    def iterate(
        self, data_product_external_id: str, limit: int | None = 10
    ) -> Iterable[list[DataProductVersionResponse]]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        for batch in self._iterate(limit=limit, endpoint_path=path):
            for item in batch:
                item.data_product_external_id = data_product_external_id
            yield batch

    def list(self, data_product_external_id: str, limit: int | None = 10) -> list[DataProductVersionResponse]:
        path = self._method_endpoint_map["list"].path.format(externalId=data_product_external_id)
        items = self._list(limit=limit, endpoint_path=path)
        for item in items:
            item.data_product_external_id = data_product_external_id
        return items
