import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal, TypeVar

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorDLQMessageResponse,
    HostedExtractorJobLogResponse,
    HostedExtractorJobMetricsResponse,
    HostedExtractorJobRequest,
    HostedExtractorJobResponse,
)

T_JobItem = TypeVar("T_JobItem", bound=BaseModel)


class HostedExtractorJobsAPI(CDFResourceAPI[HostedExtractorJobResponse]):
    _JOB_ITEMS_PAGE_LIMIT = 1000

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

    def create(self, items: Sequence[HostedExtractorJobRequest]) -> builtins.list[HostedExtractorJobResponse]:
        """Create hosted extractor jobs in CDF.

        Args:
            items: List of job request objects to create.
        Returns:
            List of created job response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[HostedExtractorJobResponse]:
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
    ) -> builtins.list[HostedExtractorJobResponse]:
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
        limit: int | None = 100,
    ) -> Iterable[builtins.list[HostedExtractorJobResponse]]:
        """Iterate over hosted extractor jobs in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of job response objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> builtins.list[HostedExtractorJobResponse]:
        """List all hosted extractor jobs in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of job response objects.
        """
        return self._list(limit=limit)

    def list_logs(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[HostedExtractorJobLogResponse]:
        """`List job logs <https://api-docs.cognite.com/20230101/tag/Jobs/operation/get_job_logs>`_, newest first.

        Args:
            job: Only return logs of the job with this external ID.
            source: Only return logs of jobs with the source with this external ID.
            destination: Only return logs of jobs with the destination with this external ID.
            limit: Maximum number of logs to return. None for all logs.

        Returns:
            List of job logs.
        """
        return self._list_job_items(
            "/hostedextractors/jobs/logs",
            PagedResponse[HostedExtractorJobLogResponse],
            {"job": job, "source": source, "destination": destination},
            limit,
        )

    def list_metrics(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[HostedExtractorJobMetricsResponse]:
        """`List job metrics <https://api-docs.cognite.com/20230101/tag/Jobs/operation/get_job_metrics>`_, newest first.

        Args:
            job: Only return metrics of the job with this external ID.
            source: Only return metrics of jobs with the source with this external ID.
            destination: Only return metrics of jobs with the destination with this external ID.
            limit: Maximum number of metrics to return. None for all metrics.

        Returns:
            List of job metrics, one per job and UTC hour.
        """
        return self._list_job_items(
            "/hostedextractors/jobs/metrics",
            PagedResponse[HostedExtractorJobMetricsResponse],
            {"job": job, "source": source, "destination": destination},
            limit,
        )

    def list_dlq_messages(
        self,
        job: str,
        min_last_updated_time: int | None = None,
        max_last_updated_time: int | None = None,
        limit: int | None = 100,
    ) -> builtins.list[HostedExtractorDLQMessageResponse]:
        """`List dead letter queue messages <https://api-docs.cognite.com/20230101/tag/Jobs/operation/get_job_dlq_messages>`_
        of a job.

        These are the messages the job failed to transform. The request fails with status 400 if the dead letter
        queue is not enabled for the job, and requires READ access to RAW in addition to hosted extractors.

        Args:
            job: External ID of the job.
            min_last_updated_time: Only return messages last updated at or after this time, in ms since epoch.
            max_last_updated_time: Only return messages last updated at or before this time, in ms since epoch.
            limit: Maximum number of messages to return. None for all messages.

        Returns:
            List of dead letter queue messages.
        """
        return self._list_job_items(
            "/hostedextractors/jobs/dlq",
            PagedResponse[HostedExtractorDLQMessageResponse],
            {
                "job": job,
                "minLastUpdatedTime": min_last_updated_time,
                "maxLastUpdatedTime": max_last_updated_time,
            },
            limit,
        )

    def _list_job_items(
        self,
        path: str,
        page_cls: type[PagedResponse[T_JobItem]],
        params: dict[str, Any],
        limit: int | None,
    ) -> builtins.list[T_JobItem]:
        items: builtins.list[T_JobItem] = []
        cursor: str | None = None
        while limit is None or len(items) < limit:
            page_limit = (
                self._JOB_ITEMS_PAGE_LIMIT if limit is None else min(self._JOB_ITEMS_PAGE_LIMIT, limit - len(items))
            )
            request = RequestMessage(
                endpoint_url=self._make_url(path),
                method="GET",
                parameters=self._filter_out_none_values({**params, "limit": page_limit, "cursor": cursor}),
            )
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            page = page_cls.model_validate_json(response.body)
            items.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return items
