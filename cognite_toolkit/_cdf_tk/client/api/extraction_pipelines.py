from collections.abc import Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.extraction_pipeline import (
    ExtractionPipelineRequest,
    ExtractionPipelineResponse,
)
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


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
                "list": Endpoint(method="GET", path="/extpipes", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[ExtractionPipelineResponse]:
        return PagedResponse[ExtractionPipelineResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
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

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ExtractionPipelineResponse]:
        """Iterate over all extraction pipelines in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of ExtractionPipelineResponse objects.
        """
        return self._iterate(
            cursor=cursor,
            limit=limit,
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
