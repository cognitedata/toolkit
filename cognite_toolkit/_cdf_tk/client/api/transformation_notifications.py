from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_notification import (
    TransformationNotificationRequest,
    TransformationNotificationResponse,
)


class TransformationNotificationsAPI(
    CDFResourceAPI[InternalId, TransformationNotificationRequest, TransformationNotificationResponse]
):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/transformations/notifications", item_limit=1000),
                "delete": Endpoint(method="POST", path="/transformations/notifications/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/transformations/notifications", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TransformationNotificationResponse]:
        return PagedResponse[TransformationNotificationResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalId]:
        return ResponseItems[InternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TransformationNotificationRequest]) -> list[TransformationNotificationResponse]:
        """Subscribe for notifications on transformation errors.

        Args:
            items: List of TransformationNotificationRequest objects to create.
        Returns:
            List of created TransformationNotificationResponse objects.
        """
        return self._request_item_response(items, "create")

    def delete(self, items: Sequence[InternalId]) -> None:
        """Delete notification subscriptions by notification ID.

        Args:
            items: List of InternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TransformationNotificationResponse]:
        """Fetch a page of transformation notification subscriptions from CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TransformationNotificationResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[TransformationNotificationResponse]]:
        """Iterate over all transformation notification subscriptions in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of TransformationNotificationResponse objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[TransformationNotificationResponse]:
        """List all transformation notification subscriptions in CDF.

        Returns:
            List of TransformationNotificationResponse objects.
        """
        return self._list(limit=limit)
