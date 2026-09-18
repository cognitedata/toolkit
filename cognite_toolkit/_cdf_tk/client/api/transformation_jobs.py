from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_job import (
    TransformationJobMetricResponse,
    TransformationJobResponse,
)


class TransformationJobsAPI(CDFResourceAPI[TransformationJobResponse]):
    """API for listing transformation jobs and their metrics."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="POST", path="/transformations/jobs/byids", item_limit=1000),
                "list": Endpoint(method="GET", path="/transformations/jobs", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TransformationJobResponse]:
        return PagedResponse[TransformationJobResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalId]:
        return ResponseItems[InternalId].model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[InternalId], ignore_unknown_ids: bool = False
    ) -> list[TransformationJobResponse]:
        """Retrieve transformation jobs from CDF.

        Args:
            items: List of InternalId objects identifying the jobs to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved TransformationJobResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def list_metrics(self, id: int) -> list[TransformationJobMetricResponse]:
        """`List job metrics by job id. <https://api-docs.cognite.com/20230101/tag/Transformation-Jobs/operation/getTransformationJobsMetrics>`_

        Args:
            id: The job id.

        Returns:
            Metrics for how many resources have been read/written by the transformation.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(f"/transformations/jobs/{id}/metrics"),
            method="GET",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ResponseItems[TransformationJobMetricResponse].model_validate_json(response.body).items

    def paginate(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TransformationJobResponse]:
        """Fetch a page of transformation jobs from CDF.

        Args:
            transformation_id: List only jobs for the specified transformation (internal ID).
            transformation_external_id: List only jobs for the specified transformation (external ID).
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TransformationJobResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params={
                "transformationId": transformation_id,
                "transformationExternalId": transformation_external_id,
            },
        )

    def iterate(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[TransformationJobResponse]]:
        """Iterate over transformation jobs in CDF.

        Args:
            transformation_id: List only jobs for the specified transformation (internal ID).
            transformation_external_id: List only jobs for the specified transformation (external ID).
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of TransformationJobResponse objects.
        """
        return self._iterate(
            limit=limit,
            params={
                "transformationId": transformation_id,
                "transformationExternalId": transformation_external_id,
            },
        )

    def list(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        limit: int | None = 100,
    ) -> list[TransformationJobResponse]:
        """List transformation jobs in CDF.

        Args:
            transformation_id: List only jobs for the specified transformation (internal ID).
            transformation_external_id: List only jobs for the specified transformation (external ID).
            limit: Maximum number of items to return.

        Returns:
            List of TransformationJobResponse objects.
        """
        return self._list(
            limit=limit,
            params={
                "transformationId": transformation_id,
                "transformationExternalId": transformation_external_id,
            },
        )
