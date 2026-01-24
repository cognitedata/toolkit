from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId


class FileMetadataAPI(CDFResourceAPI[InternalOrExternalId, FileMetadataRequest, FileMetadataResponse]):
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
    ) -> PagedResponse[FileMetadataResponse]:
        return PagedResponse[FileMetadataResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[FileMetadataRequest], overwrite: bool = False) -> list[FileMetadataResponse]:
        """Upload file metadata to CDF.

        Args:
            items: List of FileMetadataRequest objects to upload.
            overwrite: Whether to overwrite existing file metadata with the same external ID.

        Returns:
            List of created FileMetadataResponse objects.
        """
        # The Files API is different from other APIs, thus we have a custom implementation here.
        # - It only allow one item per request that is not wrapped in a "items" field.
        # - It uses a query parameter for "overwrite" instead of including it in the body
        endpoint = self._method_endpoint_map["create"]
        results: list[FileMetadataResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=item.dump(),
                parameters={"overwrite": overwrite},
            )
            response = self._http_client.request_single_retries(request)
            result = response.get_success_or_raise()
            results.append(FileMetadataResponse.model_validate_json(result.body))
        return results

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[FileMetadataResponse]:
        """Retrieve file metadata from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved FileMetadataResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[FileMetadataRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[FileMetadataResponse]:
        """Update file metadata in CDF.

        Args:
            items: List of FileMetadataRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated FileMetadataResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete file metadata from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: ClassicFilter | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[FileMetadataResponse]:
        """Iterate over file metadata in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            directory_prefix: Filter by directory prefix.
            uploaded: Filter by upload status.
            limit: Maximum number of items to return per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of FileMetadataResponse objects.
        """
        filter_: dict[str, Any] = filter.dump() if filter else {}
        if directory_prefix is not None:
            filter_["directoryPrefix"] = directory_prefix
        if uploaded is not None:
            filter_["uploaded"] = uploaded

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_ or None},
        )

    def iterate(
        self,
        filter: ClassicFilter | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[FileMetadataResponse]]:
        """Iterate over file metadata in CDF.

        Args:
            filter: Filter by data set IDs and/or asset subtree IDs.
            directory_prefix: Filter by directory prefix.
            uploaded: Filter by upload status.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of FileMetadataResponse objects.
        """
        filter_: dict[str, Any] = filter.dump() if filter else {}
        if directory_prefix is not None:
            filter_["directoryPrefix"] = directory_prefix
        if uploaded is not None:
            filter_["uploaded"] = uploaded

        return self._iterate(
            limit=limit,
            body={"filter": filter_ or None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[FileMetadataResponse]:
        """List all file metadata in CDF.

        Args:
            limit: Maximum number of items to return. None for all items.

        Returns:
            List of FileMetadataResponse objects.
        """
        return self._list(limit=limit)
