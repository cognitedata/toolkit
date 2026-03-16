import builtins
import time
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    HTTPResult,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId


class FileMetadataAPI(CDFResourceAPI[FileMetadataResponse]):
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
            api_version="alpha",
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
        self, items: Sequence[InternalId | ExternalId | InstanceId], ignore_unknown_ids: bool = False
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

    def upload_file_link(
        self, items: Sequence[ExternalId | InstanceId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FileMetadataResponse]:
        """Upload file link to CDF."""
        results: list[FileMetadataResponse] = []
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._http_client.config.create_api_url("/files/uploadlink"),
                    method="POST",
                    body_content={"items": [item.dump()]},
                )
            )
            if isinstance(response, SuccessResponse):
                results.extend(ResponseItems[FileMetadataResponse].model_validate_json(response.body).items)
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise()
        return results

    def upload_content(self, data_content: bytes, upload_url: str, mime_type: str | None = None) -> HTTPResult:
        """Uploads file content to CDF.

        Args:
            data_content: Content to be uploaded.
            upload_url: Upload URL.
            mime_type: MIME type to upload. None for no MIME type.
        """
        return self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=upload_url,
                method="PUT",
                content_type=mime_type or "application/octet-stream",
                data_content=data_content,
            )
        )

    def set_pending_ids(self, items: Sequence[PendingInstanceId]) -> builtins.list[FileMetadataResponse]:
        """Set pending instance IDs for one or more file metadata entries.

        This links asset-centric files to DM nodes that will be created
        by the syncer service.

        Args:
            items: Sequence of PendingInstanceId objects containing the pending
                instance IDs and the file id or external_id to link them to.

        Returns:
            List of updated FileMetadataResponse objects.
        """
        return self._request_item_response(items, method="retrieve", endpoint="/files/set-pending-instance-ids")

    def unlink_instance_ids(self, items: Sequence[InternalOrExternalId]) -> builtins.list[FileMetadataResponse]:
        """Unlink instance IDs from files.

        This allows a CogniteFile node in Data Modeling to be deleted
        without deleting the underlying file content.

        Args:
            items: Sequence of InternalOrExternalId identifying the files to unlink.

        Returns:
            List of updated FileMetadataResponse objects.
        """
        return self._request_item_response(items, method="retrieve", endpoint="/files/unlink-instance-ids")

    def await_file_uploaded(self, items: Sequence[InternalId], timeout_seconds: float) -> tuple[set[InternalId], float]:
        """Wait for files to be uploaded, polling their status until they are marked as uploaded or a timeout is reached.

        Args:
            items: Sequence of InternalId identifying the files to upload.
            timeout_seconds: Timeout in seconds.

        Returns:
            The identifiers of the files that were not marked as uploaded within the timeout, and the elapsed time in seconds.

        """
        to_check = set(items)
        t0 = time.perf_counter()
        sleep_time = 1.0  # seconds
        while (elapsed_time := (time.perf_counter() - t0)) < timeout_seconds:
            files = self.retrieve(list(to_check))
            to_check = {InternalId(id=file.id) for file in files if not file.uploaded}
            if not to_check:
                return set(), elapsed_time
            elapsed_time = time.perf_counter() - t0
            to_sleep = min(sleep_time, timeout_seconds - elapsed_time)
            time.sleep(to_sleep)
            sleep_time *= 2
        return to_check, elapsed_time
