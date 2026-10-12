import builtins
import time
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import IO, Any, Literal

import httpx2

from cognite_toolkit._cdf_tk.client.api._classic_aggregate import files_aggregate_count
from cognite_toolkit._cdf_tk.client.api._classic_list import (
    ObjectIds,
    TimeRange,
    classic_list_body,
    dump_model,
    int_ids,
    object_ids,
    str_ids,
    time_range,
)
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter, GeoLocationFilter, LabelFilter
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import (
    DownloadResponse,
    FileMetadataRequest,
    FileMetadataResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class _LimitedFileReader(Iterable[bytes]):
    """A file-like wrapper that limits the number of bytes read from a file stream.

    This allows httpx2 to stream content directly from disk in smaller chunks,
    without loading the entire part into memory at once. Implements Iterable[bytes]
    for compatibility with httpx2's content parameter.
    """

    _CHUNK_SIZE = 64 * 1024  # 64 KB chunks

    def __init__(self, file_stream: IO[bytes], limit: int) -> None:
        self._file_stream = file_stream
        self._limit = limit
        self._remaining = limit

    def __iter__(self) -> Iterator[bytes]:
        while self._remaining > 0:
            chunk_size = min(self._CHUNK_SIZE, self._remaining)
            data = self._file_stream.read(chunk_size)
            if not data:
                break
            self._remaining -= len(data)
            yield data

    def __len__(self) -> int:
        """Return the total size for Content-Length header."""
        return self._limit


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
                "aggregate": Endpoint(method="POST", path="/files/aggregate", item_limit=1000),
            },
            api_version="alpha",
        )
        self._create_multipart = Endpoint(method="POST", path="/files", item_limit=1, concurrency_max_workers=1)
        self._download_link = Endpoint(method="POST", path="/files/downloadlink", item_limit=10)
        # Creates a new classic file and returns multipart URLs. Body is the file object, not wrapped in "items".
        self._multipart_file_upload_link = Endpoint(method="POST", path="/files/initmultipartupload", item_limit=1)
        # Returns multipart URLs for a file that already exists (externalId or instanceId). Body is {"items": [...]}.
        self._multipart_upload_link = Endpoint(method="POST", path="/files/multiuploadlink", item_limit=1)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[FileMetadataResponse]:
        return PagedResponse[FileMetadataResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(
        self, items: Sequence[FileMetadataRequest], overwrite: bool = False
    ) -> builtins.list[FileMetadataResponse]:
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
        results: builtins.list[FileMetadataResponse] = []
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

    def upload_multi_parts(self, item: FileMetadataRequest, overwrite: bool, parts: int) -> FileMetadataResponse:
        """Upload file metadata to CDF and return multiple URLs for uploading"""
        self._validate_parts_parameter(parts)
        endpoint = self._multipart_file_upload_link
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            body_content=item.dump(),
            parameters={"overwrite": overwrite, "parts": parts},
        )
        response = self._http_client.request_single_retries(request)
        result = response.get_success_or_raise(request)
        result_item = FileMetadataResponse.model_validate_json(result.body)
        result_item.filepath = item.filepath
        return result_item

    def _validate_parts_parameter(self, parts: int) -> None:
        if not (1 <= parts <= 250):
            raise ValueError("Parts parameter must be between 1 and 250")

    def retrieve(
        self, items: Sequence[InternalId | ExternalId | InstanceId], ignore_unknown_ids: bool = False
    ) -> builtins.list[FileMetadataResponse]:
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
    ) -> builtins.list[FileMetadataResponse]:
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
        filter: ClassicFilter | dict[str, Any] | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: ObjectIds | None = None,
        root_asset_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        uploaded_time: TimeRange | None = None,
        source_created_time: TimeRange | None = None,
        source_modified_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        partition: str | None = None,
    ) -> PagedResponse[FileMetadataResponse]:
        """Fetch one page of file metadata.

        Takes the same filter arguments as :meth:`list`.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Files/operation/advancedListFiles>`_.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._list_body(
                filter=filter,
                directory_prefix=directory_prefix,
                uploaded=uploaded,
                name=name,
                mime_type=mime_type,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                root_asset_external_ids=root_asset_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                uploaded_time=uploaded_time,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                external_id_prefix=external_id_prefix,
                labels=labels,
                geo_location=geo_location,
                partition=partition,
            ),
        )

    def iterate(
        self,
        filter: ClassicFilter | dict[str, Any] | None = None,
        directory_prefix: str | None = None,
        uploaded: bool | None = None,
        limit: int | None = 100,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: ObjectIds | None = None,
        root_asset_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        uploaded_time: TimeRange | None = None,
        source_created_time: TimeRange | None = None,
        source_modified_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        partition: str | None = None,
    ) -> Iterable[builtins.list[FileMetadataResponse]]:
        """Iterate over file metadata in CDF.

        Takes the same filter arguments as :meth:`list`. ``limit`` is the maximum number of
        files to return in total; ``None`` reads every matching file.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Files/operation/advancedListFiles>`_.
        """
        return self._iterate(
            limit=limit,
            body=self._list_body(
                filter=filter,
                directory_prefix=directory_prefix,
                uploaded=uploaded,
                name=name,
                mime_type=mime_type,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                root_asset_external_ids=root_asset_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                uploaded_time=uploaded_time,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                external_id_prefix=external_id_prefix,
                labels=labels,
                geo_location=geo_location,
                partition=partition,
            ),
        )

    def list(
        self,
        limit: int | None = 100,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        directory_prefix: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: ObjectIds | None = None,
        root_asset_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        uploaded_time: TimeRange | None = None,
        source_created_time: TimeRange | None = None,
        source_modified_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        uploaded: bool | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        partition: str | None = None,
    ) -> builtins.list[FileMetadataResponse]:
        """List file metadata in CDF.

        ``filter`` is a strict filter. Individual arguments override the same field on ``filter``.
        ``partition`` is an ``"M/N"`` string. ``root_asset_ids`` accepts an internal id, external id,
        or :class:`InternalId` / :class:`ExternalId` object. The files list endpoint has no advanced filter or sort.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Files/operation/advancedListFiles>`_.
        """
        return self._list(
            limit=limit,
            body=self._list_body(
                filter=filter,
                name=name,
                directory_prefix=directory_prefix,
                mime_type=mime_type,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                root_asset_external_ids=root_asset_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                uploaded_time=uploaded_time,
                source_created_time=source_created_time,
                source_modified_time=source_modified_time,
                external_id_prefix=external_id_prefix,
                uploaded=uploaded,
                labels=labels,
                geo_location=geo_location,
                partition=partition,
            ),
        )

    @staticmethod
    def _list_body(
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        directory_prefix: str | None = None,
        mime_type: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: ObjectIds | None = None,
        root_asset_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        uploaded_time: TimeRange | None = None,
        source_created_time: TimeRange | None = None,
        source_modified_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        uploaded: bool | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        partition: str | None = None,
    ) -> dict[str, Any]:
        return classic_list_body(
            filter=filter,
            fields={
                "name": name,
                "directoryPrefix": directory_prefix,
                "mimeType": mime_type,
                "metadata": metadata,
                "assetIds": int_ids(asset_ids),
                "assetExternalIds": str_ids(asset_external_ids),
                "rootAssetIds": object_ids(root_asset_ids, root_asset_external_ids),
                "dataSetIds": object_ids(data_set_ids, data_set_external_ids),
                "assetSubtreeIds": object_ids(asset_subtree_ids, asset_subtree_external_ids),
                "source": source,
                "createdTime": time_range(created_time),
                "lastUpdatedTime": time_range(last_updated_time),
                "uploadedTime": time_range(uploaded_time),
                "sourceCreatedTime": time_range(source_created_time),
                "sourceModifiedTime": time_range(source_modified_time),
                "externalIdPrefix": external_id_prefix,
                "uploaded": uploaded,
                "labels": dump_model(labels),
                "geoLocation": dump_model(geo_location),
            },
            partition=partition,
        )

    def count(self, *, filter: ClassicFilter | dict[str, Any] | None = None) -> int:
        """Count files matching an optional filter.

        The files aggregate endpoint returns only ``count``.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Files/operation/aggregateFiles>`_.
        """
        return files_aggregate_count(self, filter=filter)

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
        if isinstance(filepath, bytes):
            content: Iterable[bytes] = (filepath,)
            content_length = len(filepath)
        elif isinstance(filepath, str):
            encoded = filepath.encode("utf-8")
            content = (encoded,)
            content_length = len(encoded)
        else:
            content_length = filepath.stat().st_size
            content = _LimitedFileReader(filepath.open("rb"), content_length)

        # Build headers with explicit Content-Length to avoid chunked transfer encoding.
        # AWS S3 and other cloud storage services don't support Transfer-Encoding: chunked.
        headers: dict[str, str] = {"Content-Length": str(content_length)}
        if mime_type:
            headers["Content-Type"] = mime_type

        response = self._http_client.request_raw_retries(
            method="PUT",
            url=upload_url,
            content=content,
            headers=headers,
        )
        if isinstance(response, FailedResponse):
            raise ToolkitAPIError(message=response.body, code=response.status_code)

        return response

    def upload_file_multiparts(
        self, filepath: Path, upload_urls: builtins.list[str], mime_type: str | None = None
    ) -> builtins.list[SuccessResponse]:
        """Upload a file to CDF in multiple parts using the provided upload URLs.

        The file is split uniformly across all upload URLs, with each part uploaded
        to its corresponding URL. Uses streaming to avoid loading entire chunks into memory.

        Args:
            filepath: The local path to the file to upload.
            upload_urls: List of URLs to upload file parts to.
            mime_type: MIME type of the file. If None, no Content-Type header is sent
                (required for GCS signed URLs that were generated without a Content-Type).

        Returns:
            List of SuccessResponse objects containing the upload response details for each part.
        """
        file_size = filepath.stat().st_size
        num_parts = len(upload_urls)
        part_size = file_size // num_parts

        results: builtins.list[SuccessResponse] = []
        with filepath.open("rb") as file_stream:
            for i, upload_url in enumerate(upload_urls):
                # Last part gets any remaining bytes
                if i == num_parts - 1:
                    current_part_size = file_size - file_stream.tell()
                else:
                    current_part_size = part_size

                # Use a stream wrapper that limits reads to the part size,
                # allowing httpx2 to stream directly from disk without loading the entire chunk into memory.
                chunk_stream = _LimitedFileReader(file_stream, current_part_size)

                # Build headers with explicit Content-Length to avoid chunked transfer encoding.
                # AWS S3 and other cloud storage services don't support Transfer-Encoding: chunked.
                headers: dict[str, str] = {"Content-Length": str(current_part_size)}
                # Only include Content-Type header if explicitly provided.
                # GCS signed URLs embed the expected Content-Type in the signature,
                # so sending a different Content-Type (or any when none was signed) causes SignatureDoesNotMatch.
                if mime_type:
                    headers["Content-Type"] = mime_type

                response = self._http_client.request_raw_retries(
                    method="PUT",
                    url=upload_url,
                    content=chunk_stream,
                    headers=headers,
                )
                if isinstance(response, FailedResponse):
                    raise ToolkitAPIError(message=response.body, code=response.status_code)
                results.append(response)

        return results

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
        results: builtins.list[FileMetadataResponse] = []
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

    def get_multipart_upload_urls(self, item: ExternalId | InstanceId, parts: int) -> FileMetadataResponse:
        """Get multipart upload URLs for one file that already exists in CDF.

        ``POST /files/initmultipartupload`` creates a new classic file and rejects an ``items`` body.
        An existing classic file or CogniteFile must use ``POST /files/multiuploadlink``.
        """
        self._validate_parts_parameter(parts)
        endpoint = self._multipart_upload_link
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(endpoint.path),
            method="POST",
            parameters={"parts": parts},
            body_content={"items": [item.dump()]},
        )
        success = self._http_client.request_single_retries(request).get_success_or_raise(request)
        items = ResponseItems[FileMetadataResponse].model_validate_json(success.body).items
        if len(items) != 1:
            raise ToolkitAPIError(
                message=f"Expected exactly one item in response, got {len(items)}",
                code=success.status_code,
            )
        return items[0]

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
        results: builtins.list[DownloadResponse] = []
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
        with httpx2.stream("GET", download_url) as response:
            if response.status_code != 200:
                raise ToolkitAPIError(
                    message=f"Download failed with status code {response.status_code}: {response.text}",
                    code=response.status_code,
                )
            with destination.open(mode="wb") as file_stream:
                for chunk in response.iter_bytes(chunk_size=8192):
                    file_stream.write(chunk)

    def complete_multipart_upload(self, item: InternalId | ExternalId | InstanceId, upload_id: str) -> SuccessResponse:
        """Complete a multipart upload for one or more file metadata entries."""
        body = item.dump()
        body["uploadId"] = upload_id
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url("/files/completemultipartupload"),
            method="POST",
            body_content=body,
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)
