from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import StreamlitFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import (
    StreamlitRequest,
    StreamlitResponse,
)


class StreamlitAPI(CDFResourceAPI[StreamlitResponse]):
    """API for managing Streamlit apps in CDF.

    Streamlit apps are stored as file metadata objects with a specific directory prefix
    and app properties encoded in the metadata field.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/files", item_limit=1, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/files/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/files/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/files/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/files/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[StreamlitResponse]:
        return PagedResponse[StreamlitResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[StreamlitRequest], overwrite: bool = False) -> list[StreamlitResponse]:
        """Create Streamlit apps in CDF.

        Args:
            items: List of StreamlitRequest objects to create.
            overwrite: Whether to overwrite existing apps with the same external ID.

        Returns:
            List of created StreamlitResponse objects.
        """
        # The Streamlit API is a wrapper of the File API, which is different from other APIs,
        # thus we have a custom implementation here.
        # - It only allow one item per request that is not wrapped in an "items" field.
        # - It uses a query parameter for "overwrite" instead of including it in the body
        endpoint = self._method_endpoint_map["create"]
        results: list[StreamlitResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.dump(),
                parameters={"overwrite": overwrite},
            )
            response = self._http_client.request_single_retries(request)
            result = response.get_success_or_raise()
            results.append(StreamlitResponse.model_validate_json(result.body))
        return results

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[StreamlitResponse]:
        """Retrieve Streamlit apps from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved StreamlitResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[StreamlitRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[StreamlitResponse]:
        """Update Streamlit apps in CDF.

        Args:
            items: List of StreamlitRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated StreamlitResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete Streamlit apps from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: StreamlitFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[StreamlitResponse]:
        """Paginate over Streamlit apps in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of StreamlitResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": (filter or StreamlitFilter()).dump()},
        )

    def iterate(
        self,
        filter: StreamlitFilter | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[StreamlitResponse]]:
        """Iterate over all Streamlit apps in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of StreamlitResponse objects.
        """
        return self._iterate(
            limit=limit,
            body={"filter": (filter or StreamlitFilter()).dump()},
        )

    def list(
        self,
        filter: StreamlitFilter | None = None,
        limit: int | None = 100,
    ) -> list[StreamlitResponse]:
        """List all Streamlit apps in CDF.

        Args:
            filter: StreamlitFilter to filter the apps.
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of StreamlitResponse objects.
        """
        return self._list(
            limit=limit,
            body={"filter": (filter or StreamlitFilter()).dump()},
        )
