from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import (
    ExtractionPipelineRequest,
    ExtractionPipelineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId


class ExtractionPipelinesAPI(
    CDFResourceAPI[InternalOrExternalId, ExtractionPipelineRequest, ExtractionPipelineResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/extpipes", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/extpipes/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/extpipes/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/extpipes/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/extpipes/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractionPipelineResponse]:
        return PagedResponse[ExtractionPipelineResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[ExtractionPipelineRequest]) -> list[ExtractionPipelineResponse]:
        """Create extraction pipelines in CDF.

        Args:
            items: List of ExtractionPipelineRequest objects to create.
        Returns:
            List of created ExtractionPipelineResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[ExtractionPipelineResponse]:
        """Retrieve extraction pipelines from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved ExtractionPipelineResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[ExtractionPipelineRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[ExtractionPipelineResponse]:
        """Update extraction pipelines in CDF.

        Args:
            items: List of ExtractionPipelineRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated ExtractionPipelineResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete extraction pipelines from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        external_id_prefix: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ExtractionPipelineResponse]:
        """Iterate over all extraction pipelines in CDF.

        Args:
            filter: Filter by data set IDs.
            external_id_prefix: Filter by external ID prefix.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of ExtractionPipelineResponse objects.
        """
        filter_body: dict[str, Any] = filter.dump() if filter else {}
        if external_id_prefix is not None:
            filter_body["externalIdPrefix"] = external_id_prefix

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_body} if filter_body else None,
        )

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        external_id_prefix: str | None = None,
        limit: int = 100,
    ) -> Iterable[list[ExtractionPipelineResponse]]:
        """Iterate over all extraction pipelines in CDF.

        Args:
            filter: Filter by data set IDs.
            external_id_prefix: Filter by external ID prefix.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of ExtractionPipelineResponse objects.
        """
        filter_body: dict[str, Any] = filter.dump() if filter else {}
        if external_id_prefix is not None:
            filter_body["externalIdPrefix"] = external_id_prefix

        return self._iterate(
            limit=limit,
            body={"filter": filter_body} if filter_body else None,
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[ExtractionPipelineResponse]:
        """List all extraction pipelines in CDF.

        Returns:
            List of ExtractionPipelineResponse objects.
        """
        return self._list(limit=limit)
