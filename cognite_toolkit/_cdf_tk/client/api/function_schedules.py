"""Function Schedules API for managing CDF function schedules.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Function-schedules/operation/postFunctionSchedules
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import EpochTimestampRange
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleData,
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)


class FunctionSchedulesAPI(CDFResourceAPI[FunctionScheduleResponse]):
    """API for managing CDF function schedules.

    Note: Function schedules do not support update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/functions/schedules", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/functions/schedules/byids", item_limit=10_000),
                "delete": Endpoint(method="POST", path="/functions/schedules/delete", item_limit=10_000),
                # The list endpoint has no cursor, so this is the most schedules list(limit=None) can return.
                "list": Endpoint(method="POST", path="/functions/schedules/list", item_limit=10_000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionScheduleResponse]:
        return PagedResponse[FunctionScheduleResponse].model_validate_json(response.body)

    def create(self, items: Sequence[FunctionScheduleRequest]) -> builtins.list[FunctionScheduleResponse]:
        """Create function schedules in CDF.

        Args:
            items: List of FunctionScheduleRequest objects to create.

        Returns:
            List of created FunctionScheduleResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FunctionScheduleResponse]:
        """Retrieve function schedules from CDF by ID.

        Args:
            items: List of InternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved FunctionScheduleResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete function schedules from CDF.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def _create_list_request_body(
        self,
        function_id: int | None,
        function_external_id: str | None = None,
        name: str | None = None,
        created_time: EpochTimestampRange | None = None,
        cron_expression: str | None = None,
    ) -> dict[str, Any] | None:
        if function_id is not None and function_external_id is not None:
            raise ValueError("Only one of function_id and function_external_id can be set.")
        filter: dict[str, Any] = {}
        if function_id is not None:
            filter["functionId"] = function_id
        if function_external_id is not None:
            filter["functionExternalId"] = function_external_id
        if name is not None:
            filter["name"] = name
        if created_time is not None:
            filter["createdTime"] = created_time.dump()
        if cron_expression is not None:
            filter["cronExpression"] = cron_expression
        return {"filter": filter} if filter else None

    def input_data(self, schedule_id: int) -> FunctionScheduleData:
        """Retrieve the input data to the associated function schedule..

        Args:
            schedule_id: The schedule ID to retrieve.

        Returns:
            The input data to the associated function schedule.
        """
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(f"/functions/schedules/{schedule_id}/input_data"),
            method="GET",
        )
        success = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return FunctionScheduleData.model_validate_json(success.body)

    def paginate(
        self,
        function_id: int | None = None,
        function_external_id: str | None = None,
        name: str | None = None,
        created_time: EpochTimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FunctionScheduleResponse]:
        """Get a page of function schedules from CDF.

        Args:
            function_id: Only return schedules of the function with this ID.
            function_external_id: Only return schedules of the function with this external ID.
            name: Only return schedules with this name.
            created_time: Only return schedules created in this time range.
            cron_expression: Only return schedules with this cron expression.
            limit: Maximum number of function schedules to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of FunctionScheduleResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._create_list_request_body(function_id, function_external_id, name, created_time, cron_expression),
        )

    def iterate(
        self,
        function_id: int | None = None,
        function_external_id: str | None = None,
        name: str | None = None,
        created_time: EpochTimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = None,
    ) -> Iterable[builtins.list[FunctionScheduleResponse]]:
        """Iterate over all function schedules in CDF.

        Args:
            function_id: Only return schedules of the function with this ID.
            function_external_id: Only return schedules of the function with this external ID.
            name: Only return schedules with this name.
            created_time: Only return schedules created in this time range.
            cron_expression: Only return schedules with this cron expression.
            limit: Maximum total number of function schedules to return.

        Returns:
            Iterable of lists of FunctionScheduleResponse objects.
        """
        return self._iterate(
            limit=limit,
            body=self._create_list_request_body(function_id, function_external_id, name, created_time, cron_expression),
        )

    def list(
        self,
        function_id: int | None = None,
        function_external_id: str | None = None,
        name: str | None = None,
        created_time: EpochTimestampRange | None = None,
        cron_expression: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[FunctionScheduleResponse]:
        """List all function schedules in CDF.

        Args:
            function_id: Only return schedules of the function with this ID.
            function_external_id: Only return schedules of the function with this external ID.
            name: Only return schedules with this name.
            created_time: Only return schedules created in this time range.
            cron_expression: Only return schedules with this cron expression.
            limit: Maximum total number of function schedules to return.

        Returns:
            List of FunctionScheduleResponse objects.
        """
        return self._list(
            limit=limit,
            body=self._create_list_request_body(function_id, function_external_id, name, created_time, cron_expression),
        )
