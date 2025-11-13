from collections.abc import Sequence

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.streams import StreamRequest, StreamResponse
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemsRequest, ParamRequest


class StreamsAPI:
    ENDPOINT = "/streams"

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def create(self, items: Sequence[StreamRequest]) -> list[StreamResponse]:
        """Create one or more streams.

        Args:
            items: Sequence of StreamRequest items to create.

        Returns:
            List of created StreamResponse items.
        """
        responses = self._http_client.request_with_retries(
            ItemsRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="POST",
                items=list(items),
            )
        )
        responses.raise_for_status()
        return PagedResponse[StreamResponse].model_validate(responses.get_first_body()).items

    def delete(self, external_id: str) -> None:
        """Delete stream using its external ID.

        Args:
            external_id: External ID of the stream to delete.
        """
        responses = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/{external_id}"),
                method="DELETE",
            )
        )
        responses.raise_for_status()

    def list(self) -> list[StreamResponse]:
        """List streams.

        Returns:
            StreamResponseList containing the listed streams.
        """
        responses = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="GET",
            )
        )
        responses.raise_for_status()
        return PagedResponse[StreamResponse].model_validate(responses.get_first_body()).items

    def retrieve(self, external_id: str, include_statistics: bool = True) -> StreamResponse:
        """Retrieve a stream by its external ID.

        Args:
            external_id: External ID of the stream to retrieve.
            include_statistics: Whether to include usage statistics in the response.
        Returns:
            StreamResponse item.
        """
        responses = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=self._config.create_api_url(f"{self.ENDPOINT}/{external_id}"),
                method="GET",
                parameters={"includeStatistics": include_statistics},
            )
        )
        responses.raise_for_status()
        response_body = responses.get_first_body()
        return StreamResponse.model_validate(response_body)
