"""Data Products API for managing CDF data products."""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse

from .data_product_versions import DataProductVersionsAPI


class DataProductsAPI(CDFResourceAPI[DataProductResponse]):
    """API for managing CDF data products."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/dataproducts/{externalId}", item_limit=1),
                "update": Endpoint(method="POST", path="/dataproducts/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/dataproducts/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/dataproducts", item_limit=1000),
            },
            api_version="alpha",
        )
        self.versions = DataProductVersionsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataProductResponse]:
        return PagedResponse[DataProductResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        """Create data products in CDF.

        Args:
            items: List of DataProductRequest objects to create.
        Returns:
            List of created DataProductResponse objects.

        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, external_ids: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[DataProductResponse]:
        """Retrieve data products by external ID.

        The API only supports single-item GET at /dataproducts/{externalId}.

        Args:
            external_ids: List of ExternalId objects of the data products to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs. If False, an error will be raised
                if any of the provided IDs do not exist.
        Returns:
            List of retrieved DataProductResponse objects.

        """
        results: list[DataProductResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for ext_id in external_ids:
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(endpoint.path.format(externalId=ext_id.external_id)),
                    method=endpoint.method,
                    api_version=self._api_version,
                )
            )
            if isinstance(response, SuccessResponse):
                results.append(DataProductResponse.model_validate_json(response.body))
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise()
        return results

    def update(
        self, items: Sequence[DataProductRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[DataProductResponse]:
        return self._update(items, mode=mode)

    def delete(self, external_ids: Sequence[ExternalId]) -> None:
        self._request_no_response(external_ids, "delete")

    def paginate(self, limit: int = 10, cursor: str | None = None) -> PagedResponse[DataProductResponse]:
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 10) -> Iterable[list[DataProductResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 10) -> list[DataProductResponse]:
        return self._list(limit=limit)
