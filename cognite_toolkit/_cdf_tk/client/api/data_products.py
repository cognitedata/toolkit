"""Data Products API for managing CDF data products."""

import builtins
from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse

from .data_product_roles import DataProductRolesAPI
from .data_product_versions import DataProductVersionsAPI


class DataProductsAPI(CDFResourceAPI[DataProductResponse]):
    """API for managing CDF data products."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/dataproducts/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/dataproducts/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/dataproducts/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/dataproducts", item_limit=100),
            },
            api_version="alpha",
        )
        self.versions = DataProductVersionsAPI(http_client)
        self.roles = DataProductRolesAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataProductResponse]:
        return PagedResponse[DataProductResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DataProductRequest]) -> builtins.list[DataProductResponse]:
        """Create data products in CDF.

        Args:
            items: List of DataProductRequest objects to create.
        Returns:
            List of created DataProductResponse objects.

        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, external_ids: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[DataProductResponse]:
        """Retrieve data products by external ID.

        The byids endpoint has no ignoreUnknownIds flag, so unknown IDs are skipped by splitting failed requests.

        Args:
            external_ids: List of ExternalId objects of the data products to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs. If False, an error will be raised
                if any of the provided IDs do not exist.
        Returns:
            List of retrieved DataProductResponse objects.

        """
        if ignore_unknown_ids:
            return self._request_item_split_retries(external_ids, "retrieve")
        return self._request_item_response(external_ids, "retrieve")

    def update(
        self, items: Sequence[DataProductRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[DataProductResponse]:
        return self._update(items, mode=mode)

    def delete(self, external_ids: Sequence[ExternalId]) -> None:
        self._request_no_response(external_ids, "delete")

    def paginate(self, limit: int = 10, cursor: str | None = None) -> PagedResponse[DataProductResponse]:
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 10) -> Iterable[builtins.list[DataProductResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 10) -> builtins.list[DataProductResponse]:
        return self._list(limit=limit)
