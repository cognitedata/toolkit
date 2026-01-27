from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse


class StreamsAPI(CDFResourceAPI[ExternalId, StreamRequest, StreamResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/streams", item_limit=1),
                "delete": Endpoint(method="POST", path="/streams/delete", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/streams/{streamId}", item_limit=1),
                "list": Endpoint(method="GET", path="/streams", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
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

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete streams using their external IDs.

        Args:
            items: Sequence of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        if ignore_unknown_ids:
            # The endpoint does not support ignoreUnknownIds, so we have to do it on the client side
            return self._request_item_split_retries_no_response(items, "delete")
        else:
            return self._request_no_response(items, "delete")

    def retrieve(
        self, items: Sequence[ExternalId], include_statistics: bool = False, ignore_unknown_ids: bool = False
    ) -> list[StreamResponse]:
        """Retrieve streams by their external IDs.

        Note: The streams API only supports retrieving one stream at a time via path parameter.

        Args:
            items: Sequence of ExternalId objects to retrieve.
            include_statistics: Whether to include usage statistics in the response.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of StreamResponse items.
        """
        results: list[StreamResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(endpoint.path.format(streamId=item.external_id)),
                    method=endpoint.method,
                    parameters={"includeStatistics": include_statistics},
                )
            )
            if isinstance(response, SuccessResponse):
                results.append(StreamResponse.model_validate(response.body_json))
            elif ignore_unknown_ids:
                continue
            _ = response.get_success_or_raise()
        return results

    def list(self) -> list[StreamResponse]:
        """List all streams.

        Note: The streams API does not support pagination.

        Returns:
            List of StreamResponse items.
        """
        return self._list(limit=None)
