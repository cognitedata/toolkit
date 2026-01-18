from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse


class StreamsAPI(CDFResourceAPI[ExternalId, StreamRequest, StreamResponse]):
    ENDPOINT = "/streams"

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/streams", item_limit=1000, concurrency_max_workers=1),
                # Note: list uses GET without pagination
                "list": Endpoint(method="GET", path="/streams", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[StreamResponse]:
        return PagedResponse[StreamResponse].model_validate_json(response.body)

    def create(self, items: Sequence[StreamRequest]) -> list[StreamResponse]:
        """Create one or more streams.

        Args:
            items: Sequence of StreamRequest items to create.

        Returns:
            List of created StreamResponse items.
        """
        return self._request_item_response(items, "create")

    def delete(self, external_id: str) -> None:
        """Delete stream using its external ID.

        Args:
            external_id: External ID of the stream to delete.
        """
        response = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._make_url(f"{self.ENDPOINT}/{external_id}"),
                method="DELETE",
            )
        )
        _ = response.get_success_or_raise()

    def list(self) -> list[StreamResponse]:
        """List streams.

        Returns:
            StreamResponseList containing the listed streams.
        """
        response = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._make_url(self.ENDPOINT),
                method="GET",
            )
        )
        success = response.get_success_or_raise()
        return PagedResponse[StreamResponse].model_validate(success.body_json).items

    def retrieve(self, external_id: str, include_statistics: bool = True) -> StreamResponse:
        """Retrieve a stream by its external ID.

        Args:
            external_id: External ID of the stream to retrieve.
            include_statistics: Whether to include usage statistics in the response.
        Returns:
            StreamResponse item.
        """
        response = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._make_url(f"{self.ENDPOINT}/{external_id}"),
                method="GET",
                parameters={"includeStatistics": include_statistics},
            )
        )
        success = response.get_success_or_raise()
        return StreamResponse.model_validate(success.body_json)
