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
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/streams", item_limit=1000),
                # Note: list uses GET without pagination
                "list": Endpoint(method="GET", path="/streams", item_limit=1000),
                # Note: retrieve uses path parameter GET /streams/{externalId}
                "retrieve": Endpoint(method="GET", path="/streams", item_limit=1),
                # Note: delete uses path parameter POST /streams/{externalId}/delete
                "delete": Endpoint(method="POST", path="/streams", item_limit=1),
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

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete streams using their external IDs.

        Note: The streams API only supports deleting one stream at a time via path parameter.

        Args:
            items: Sequence of ExternalId objects to delete.
        """
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage2(
                    endpoint_url=self._make_url(f"/streams/{item.external_id}/delete"),
                    method="POST",
                )
            )
            _ = response.get_success_or_raise()

    def list(self) -> list[StreamResponse]:
        """List all streams.

        Note: The streams API does not support pagination.

        Returns:
            List of StreamResponse items.
        """
        response = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._make_url("/streams"),
                method="GET",
            )
        )
        success = response.get_success_or_raise()
        return PagedResponse[StreamResponse].model_validate(success.body_json).items

    def retrieve(self, items: Sequence[ExternalId], include_statistics: bool = True) -> list[StreamResponse]:
        """Retrieve streams by their external IDs.

        Note: The streams API only supports retrieving one stream at a time via path parameter.

        Args:
            items: Sequence of ExternalId objects to retrieve.
            include_statistics: Whether to include usage statistics in the response.

        Returns:
            List of StreamResponse items.
        """
        results: list[StreamResponse] = []
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage2(
                    endpoint_url=self._make_url(f"/streams/{item.external_id}"),
                    method="GET",
                    parameters={"includeStatistics": include_statistics},
                )
            )
            success = response.get_success_or_raise()
            results.append(StreamResponse.model_validate(success.body_json))
        return results
