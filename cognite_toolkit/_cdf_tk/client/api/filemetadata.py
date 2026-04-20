import builtins
import io
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Literal

import httpx

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import (
    DownloadResponse,
    FileMetadataRequest,
    FileMetadataResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


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
        self._download_link = Endpoint(method="POST", path="/files/downloadlink", item_limit=10)

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
            result = response.get_success_or_raise(request)
            file_response = FileMetadataResponse.model_validate_json(result.body)
            file_response.filepath = item.filepath
            results.append(file_response)
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
            time.sleep(max(0, to_sleep))
            sleep_time *= 2
        return to_check, elapsed_time

    def upload_file(
        self, filepath: Path | str | bytes, upload_url: str, mime_type: str | None = None
    ) -> SuccessResponse:
        """Upload a file to CDF using streaming to avoid loading entire file into memory.

        Args:
            filepath: The local path to the file to upload, or raw bytes content.
            upload_url: The URL to upload the file to.
            mime_type: MIME type of the file. Defaults to "application/octet-stream".

        Returns:
            SuccessResponse object containing the upload response details.
        """
        # Todo: If file size is above 5000 MB - 5,000,000,000 bytes, do a multipart file upload.
        content_type = mime_type or "application/octet-stream"

        if isinstance(filepath, bytes):
            file_stream: io.IOBase = io.BytesIO(filepath)
        elif isinstance(filepath, str):
            file_stream = io.StringIO(filepath)
        else:
            file_stream = filepath.open("rb")

        try:
            response = httpx.put(upload_url, content=file_stream, headers={"Content-Type": content_type})
            if response.status_code not in (200, 201):
                raise ToolkitAPIError(
                    message=f"Upload failed with status code {response.status_code}: {response.text}",
                    code=response.status_code,
                )
            return SuccessResponse(status_code=response.status_code, body=response.text, content=response.content)
        finally:
            file_stream.close()

    def get_upload_url(
        self, items: Sequence[ExternalId | InstanceId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FileMetadataResponse]:
        """Get a URL to upload a file to CDF for one or more file metadata entries.

        Args:
            items: Sequence of InternalId identifying the files to upload.
            ignore_unknown_ids: Whether to ignore unknown identifiers.

        Returns:
            List of updated FileMetadataResponse objects.

        """
        results: list[FileMetadataResponse] = []
        for item in items:
            # The API only supports one
            request = RequestMessage(
                endpoint_url=self._http_client.config.create_api_url("/files/uploadlink"),
                method="POST",
                body_content={"items": [item.dump()]},
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                results.extend(ResponseItems[FileMetadataResponse].model_validate_json(response.body).items)
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise(request)
        return results

    def get_download_url(
        self, items: Sequence[InternalId], extended_expiration: bool = False
    ) -> builtins.list[DownloadResponse]:
        """Get a URL to download a file to CDF for one or more file metadata entries.

        Args:
            items: Sequence of InternalId identifying the files to download.
            extended_expiration: If True, the expiration will be 1 hour instead of 30 seconds for the
                the download URL.

        Returns:
                List of DownloadResponse objects containing the download URLs.
        """
        results: list[DownloadResponse] = []
        for chunk in chunker_sequence(items, self._download_link.item_limit):
            request = RequestMessage(
                endpoint_url=self._http_client.config.create_api_url(self._download_link.path),
                method=self._download_link.method,
                body_content={"items": [item.dump() for item in chunk]},
                parameters={"extendedExpiration": extended_expiration},
            )
            success = self._http_client.request_single_retries(request).get_success_or_raise(request)
            results.extend(ResponseItems[DownloadResponse].model_validate_json(success.body).items)
        return results

    def download_file(self, download_url: str, destination: Path) -> None:
        """Download a file from CDF using a download URL.

        Args:
            download_url: The URL to download the file from.
            destination: The local path to save the downloaded file to.
        """
        with httpx.stream("GET", download_url) as response:
            if response.status_code != 200:
                raise ToolkitAPIError(
                    message=f"Download failed with status code {response.status_code}: {response.text}",
                    code=response.status_code,
                )
            with destination.open(mode="wb") as file_stream:
                for chunk in response.iter_bytes(chunk_size=8192):
                    file_stream.write(chunk)
