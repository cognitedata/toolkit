import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.api._classic_list import TimeRange, time_range
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_run import (
    ExtractionPipelineRunRequest,
    ExtractionPipelineRunResponse,
    ExtractionPipelineRunStatus,
)


class ExtractionPipelineRunsAPI(CDFResourceAPI[ExtractionPipelineRunResponse]):
    """The run history of extraction pipelines, which CDF uses to report whether an extractor is healthy."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/extpipes/runs", item_limit=1000),
                "list": Endpoint(method="POST", path="/extpipes/runs/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ExtractionPipelineRunResponse]:
        return PagedResponse[ExtractionPipelineRunResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ExtractionPipelineRunRequest]) -> builtins.list[ExtractionPipelineRunResponse]:
        """Report runs of extraction pipelines.

        Args:
            items: The runs to report.

        Returns:
            The created runs.
        """
        return self._request_item_response(items, "create")

    @staticmethod
    def _list_body(
        external_id: str,
        statuses: Sequence[ExtractionPipelineRunStatus] | None,
        created_time: TimeRange | None,
        message_substring: str | None,
    ) -> dict[str, Any]:
        filter_: dict[str, Any] = {"externalId": external_id}
        if statuses:
            filter_["statuses"] = list(statuses)
        if dumped_time := time_range(created_time):
            filter_["createdTime"] = dumped_time
        if message_substring is not None:
            filter_["message"] = {"substring": message_substring}
        return {"filter": filter_}

    def paginate(
        self,
        external_id: str,
        statuses: Sequence[ExtractionPipelineRunStatus] | None = None,
        created_time: TimeRange | None = None,
        message_substring: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ExtractionPipelineRunResponse]:
        """Fetch a page of the runs of an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            statuses: Only return runs with these statuses.
            created_time: Only return runs created in this time range.
            message_substring: Only return runs whose message contains this text.
            limit: Maximum number of runs to return.
            cursor: Cursor for pagination.

        Returns:
            A page of runs.
        """
        body = self._list_body(external_id, statuses, created_time, message_substring)
        return self._paginate(limit=limit, cursor=cursor, body=body)

    def iterate(
        self,
        external_id: str,
        statuses: Sequence[ExtractionPipelineRunStatus] | None = None,
        created_time: TimeRange | None = None,
        message_substring: str | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[ExtractionPipelineRunResponse]]:
        """Iterate over the runs of an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            statuses: Only return runs with these statuses.
            created_time: Only return runs created in this time range.
            message_substring: Only return runs whose message contains this text.
            limit: Maximum number of runs to return. None returns all.

        Returns:
            Pages of runs.
        """
        body = self._list_body(external_id, statuses, created_time, message_substring)
        return self._iterate(limit=limit, body=body)

    def list(
        self,
        external_id: str,
        statuses: Sequence[ExtractionPipelineRunStatus] | None = None,
        created_time: TimeRange | None = None,
        message_substring: str | None = None,
        limit: int | None = 100,
    ) -> builtins.list[ExtractionPipelineRunResponse]:
        """List the runs of an extraction pipeline.

        Args:
            external_id: External ID of the extraction pipeline.
            statuses: Only return runs with these statuses.
            created_time: Only return runs created in this time range.
            message_substring: Only return runs whose message contains this text.
            limit: Maximum number of runs to return. None returns all.

        Returns:
            The runs.
        """
        body = self._list_body(external_id, statuses, created_time, message_substring)
        return self._list(limit=limit, body=body)
