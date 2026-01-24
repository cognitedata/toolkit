from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorJobRequest,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class HostedExtractorJobsAPI(CDFResourceAPI[ExternalId, HostedExtractorJobRequest, HostedExtractorJobResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/hostedextractors/jobs", item_limit=10),
                "retrieve": Endpoint(method="POST", path="/hostedextractors/jobs/byids", item_limit=100),
                "update": Endpoint(method="POST", path="/hostedextractors/jobs/update", item_limit=10),
                "delete": Endpoint(method="POST", path="/hostedextractors/jobs/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/hostedextractors/jobs", item_limit=100),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[HostedExtractorJobResponse]:
        return PagedResponse[HostedExtractorJobResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[HostedExtractorJobRequest]) -> list[HostedExtractorJobResponse]:
        """Create hosted extractor jobs in CDF.

        Args:
            items: List of job request objects to create.
        Returns:
            List of created job response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[HostedExtractorJobResponse]:
        """Retrieve hosted extractor jobs from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved job response objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[HostedExtractorJobRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[HostedExtractorJobResponse]:
        """Update hosted extractor jobs in CDF.

        Args:
            items: List of job request objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated job response objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete hosted extractor jobs from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[HostedExtractorJobResponse]:
        """Iterate over hosted extractor jobs in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of job response objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[HostedExtractorJobResponse]]:
        """Iterate over hosted extractor jobs in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of job response objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[HostedExtractorJobResponse]:
        """List all hosted extractor jobs in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of job response objects.
        """
        return self._list(limit=limit)
