"""Data Products API for managing CDF data products."""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class DataProductsAPI(CDFResourceAPI[ExternalId, DataProductRequest, DataProductResponse]):
    """API for managing CDF data products."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dataproducts", item_limit=1),
                "update": Endpoint(method="POST", path="/dataproducts/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/dataproducts/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/dataproducts", item_limit=1000),
            },
        )

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

    def retrieve(self, external_ids: Sequence[str], ignore_unknown_ids: bool = False) -> list[DataProductResponse]:
        """Retrieve data products by external ID. The API only supports single-item GET.

        Args:
            external_ids: List of external IDs of the data products to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs. If False, an error will be raised if any of the provided IDs do not exist.
        Returns:
            List of retrieved DataProductResponse objects.

        """

        results: list[DataProductResponse] = []
        for external_id in external_ids:
            request = RequestMessage(
                endpoint_url=self._make_url(f"/dataproducts/{external_id}"),
                method="GET",
                body_content={},
            )
            try:
                response = self._http_client.request_single_retries(request)
                success = response.get_success_or_raise()
                result = DataProductResponse.model_validate_json(success.body)
                results.append(result)
            except Exception:
                if not ignore_unknown_ids:
                    raise
        return results

    def update(
        self, items: Sequence[DataProductRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[DataProductResponse]:
        return self._update(items, mode=mode)

    def delete(self, external_ids: Sequence[str], ignore_unknown_ids: bool = False) -> None:
        items = [ExternalId(external_id=ext_id) for ext_id in external_ids]
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[DataProductResponse]:
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = None) -> Iterable[list[DataProductResponse]]:
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[DataProductResponse]:
        return self._list(limit=limit)
