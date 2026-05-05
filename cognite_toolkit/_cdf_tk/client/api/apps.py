"""AppsAPI: Dune apps deployed via the CDF App Hosting API."""

import json
import uuid
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._data_classes import SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


def _build_multipart(fields: dict[str, str], zip_bytes: bytes, filename: str = "app.zip") -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.append(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n'
            f"\r\n"
            f"{value}\r\n".encode()
        )
    parts.append(
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/zip\r\n"
        f"\r\n".encode()
        + zip_bytes
        + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


class AppsAPI:
    """Client for the CDF App Hosting API (POST /apphosting/...)."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _url(self, path: str) -> str:
        return self._http_client.config.create_api_url(path)

    def ensure_app(self, item: AppRequest) -> None:
        """POST /apphosting/apps — create the app if it does not exist; 409 = already exists (idempotent)."""
        request = RequestMessage(
            endpoint_url=self._url("/apphosting/apps"),
            method="POST",
            body_content={"items": [item.dump()]},
        )
        result = self._http_client.request_single_retries(request)
        if isinstance(result, SuccessResponse) or result.status_code == 409:
            return
        result.get_success_or_raise(request)

    def upload_version(
        self,
        app_external_id: str,
        version_tag: str,
        entry_path: str,
        zip_bytes: bytes,
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions — multipart upload of the zipped app."""
        body, content_type = _build_multipart(
            fields={"version": version_tag, "entryPath": entry_path},
            zip_bytes=zip_bytes,
        )
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{app_external_id}/versions"),
            method="POST",
            data_content=body,
            content_type=content_type,
            disable_gzip=True,
        )
        result = self._http_client.request_single_retries(request)
        # 409 means this exact version already exists — treat as success (idempotent).
        if isinstance(result, SuccessResponse) or result.status_code == 409:
            return
        result.get_success_or_raise(request)

    def publish(self, app_external_id: str, version_tag: str) -> None:
        """POST /apphosting/apps/{externalId}/versions/update — flip version to PUBLISHED + alias ACTIVE."""
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{app_external_id}/versions/update"),
            method="POST",
            body_content={
                "items": [
                    {
                        "version": version_tag,
                        "update": {
                            "lifecycleState": {"set": "PUBLISHED"},
                            "alias": {"set": "ACTIVE"},
                        },
                    }
                ]
            },
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[AppResponse]:
        """GET /apphosting/apps/{appExternalId} for each id."""
        results: list[AppResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._url(f"/apphosting/apps/{item.external_id}"),
                method="GET",
            )
            result = self._http_client.request_single_retries(request)
            if isinstance(result, SuccessResponse):
                    data = json.loads(result.body)
                results.append(
                    AppResponse(
                        app_external_id=data["externalId"],
                        version_tag="",
                        name=data.get("name", ""),
                        description=data.get("description"),
                    )
                )
            elif result.status_code == 404 and ignore_unknown_ids:
                continue
            else:
                result.get_success_or_raise(request)
        return results

    def iterate(self, limit: int | None = 100) -> Iterable[list[AppResponse]]:
        """POST /apphosting/apps/list — paginated list of all apps."""
        cursor: str | None = None
        page_limit = min(limit, 1000) if limit is not None else 1000
        fetched = 0
        while True:
            body: dict = {"limit": page_limit}
            if cursor:
                body["cursor"] = cursor
            request = RequestMessage(
                endpoint_url=self._url("/apphosting/apps/list"),
                method="POST",
                body_content=body,
            )
            result = self._http_client.request_single_retries(request)
            if not isinstance(result, SuccessResponse):
                result.get_success_or_raise(request)
                break

            data = json.loads(result.body)
            page_items = [
                AppResponse(
                    app_external_id=item["externalId"],
                    version_tag="",
                    name=item.get("name", ""),
                    description=item.get("description"),
                )
                for item in data.get("items", [])
            ]
            if page_items:
                yield page_items
                fetched += len(page_items)

            cursor = data.get("nextCursor")
            if not cursor or (limit is not None and fetched >= limit):
                break

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """POST /apphosting/apps/delete — soft-delete apps and all their versions."""
        if not items:
            return
        request = RequestMessage(
            endpoint_url=self._url("/apphosting/apps/delete"),
            method="POST",
            body_content={
                "items": [{"externalId": item.external_id} for item in items],
                "ignoreUnknownIds": ignore_unknown_ids,
            },
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)
