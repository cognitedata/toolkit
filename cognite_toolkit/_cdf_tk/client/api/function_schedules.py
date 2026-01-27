"""Function Schedules API for managing CDF function schedules.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Function-schedules/operation/postFunctionSchedules
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId


class FunctionSchedulesAPI(CDFResourceAPI[InternalId, FunctionScheduleRequest, FunctionScheduleResponse]):
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
                "list": Endpoint(method="POST", path="/functions/schedules/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FunctionScheduleResponse]:
        return PagedResponse[FunctionScheduleResponse].model_validate_json(response.body)

    def create(self, items: Sequence[FunctionScheduleRequest]) -> list[FunctionScheduleResponse]:
        """Create function schedules in CDF.

        Args:
            items: List of FunctionScheduleRequest objects to create.

        Returns:
            List of created FunctionScheduleResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId], ignore_unknown_ids: bool = False) -> list[FunctionScheduleResponse]:
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

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FunctionScheduleResponse]:
        """Get a page of function schedules from CDF.

        Args:
            limit: Maximum number of function schedules to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of FunctionScheduleResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[list[FunctionScheduleResponse]]:
        """Iterate over all function schedules in CDF.

        Args:
            limit: Maximum total number of function schedules to return.

        Returns:
            Iterable of lists of FunctionScheduleResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[FunctionScheduleResponse]:
        """List all function schedules in CDF.

        Args:
            limit: Maximum total number of function schedules to return.

        Returns:
            List of FunctionScheduleResponse objects.
        """
        return self._list(limit=limit)
