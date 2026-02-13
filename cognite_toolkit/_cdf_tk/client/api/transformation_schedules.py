from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import (
    TransformationScheduleRequest,
    TransformationScheduleResponse,
)


class TransformationSchedulesAPI(
    CDFResourceAPI[InternalOrExternalId, TransformationScheduleRequest, TransformationScheduleResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/transformations/schedules", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/transformations/schedules/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/transformations/schedules/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/transformations/schedules/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/transformations/schedules", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TransformationScheduleResponse]:
        return PagedResponse[TransformationScheduleResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TransformationScheduleRequest]) -> list[TransformationScheduleResponse]:
        """Schedule transformations with the specified configurations.

        Args:
            items: List of TransformationScheduleRequest objects to create.
        Returns:
            List of created TransformationScheduleResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[TransformationScheduleResponse]:
        """Retrieve transformation schedules from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved TransformationScheduleResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def update(
        self, items: Sequence[TransformationScheduleRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[TransformationScheduleResponse]:
        """Update transformation schedules in CDF.

        Args:
            items: List of TransformationScheduleRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated TransformationScheduleResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Unschedule transformations from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TransformationScheduleResponse]:
        """Fetch a page of transformation schedules from CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TransformationScheduleResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[TransformationScheduleResponse]]:
        """Iterate over all transformation schedules in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of TransformationScheduleResponse objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[TransformationScheduleResponse]:
        """List all transformation schedules in CDF.

        Returns:
            List of TransformationScheduleResponse objects.
        """
        return self._list(limit=limit)
