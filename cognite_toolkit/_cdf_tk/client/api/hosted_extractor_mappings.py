from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import (
    HostedExtractorMappingRequest,
    HostedExtractorMappingResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class HostedExtractorMappingsAPI(
    CDFResourceAPI[ExternalId, HostedExtractorMappingRequest, HostedExtractorMappingResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/hostedextractors/mappings", item_limit=10),
                "retrieve": Endpoint(method="POST", path="/hostedextractors/mappings/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/hostedextractors/mappings/update", item_limit=10),
                "delete": Endpoint(method="POST", path="/hostedextractors/mappings/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/hostedextractors/mappings", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[HostedExtractorMappingResponse]:
        return PagedResponse[HostedExtractorMappingResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[HostedExtractorMappingRequest]) -> list[HostedExtractorMappingResponse]:
        """Create hosted extractor mappings in CDF.

        Args:
            items: List of mapping request objects to create.
        Returns:
            List of created mapping response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[HostedExtractorMappingResponse]:
        """Retrieve hosted extractor mappings from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved mapping response objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[HostedExtractorMappingRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[HostedExtractorMappingResponse]:
        """Update hosted extractor mappings in CDF.

        Args:
            items: List of mapping request objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated mapping response objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False, force: bool | None = None) -> None:
        """Delete hosted extractor mappings from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
            force: Delete any jobs associated with each item.
        """
        extra_body: dict[str, Any] = {"ignoreUnknownIds": ignore_unknown_ids}
        if force is not None:
            extra_body["force"] = force

        self._request_no_response(items, "delete", extra_body=extra_body)

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[HostedExtractorMappingResponse]:
        """Iterate over hosted extractor mappings in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of mapping response objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[HostedExtractorMappingResponse]]:
        """Iterate over hosted extractor mappings in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of mapping response objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[HostedExtractorMappingResponse]:
        """List all hosted extractor mappings in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of mapping response objects.
        """
        return self._list(limit=limit)
