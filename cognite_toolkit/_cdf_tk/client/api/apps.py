"""AppsAPI: Dune apps deployed via the CDF App Hosting API."""

import json
import uuid
from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId, ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


def _build_multipart(fields: dict[str, str], zip_bytes: bytes, filename: str = "app.zip") -> tuple[bytes, str]:
    boundary = uuid.uuid4().hex
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
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


_LIFECYCLE_ORDER = ["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"]


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
        if isinstance(result, SuccessResponse) or (isinstance(result, FailedResponse) and result.status_code == 409):
            return
        result.get_success_or_raise(request)

    def upload_version(
        self,
        external_id: str,
        version: str,
        entrypoint: str,
        zip_bytes: bytes,
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions — multipart upload of the zipped app."""
        body, content_type = _build_multipart(
            fields={"version": version, "entryPath": entrypoint},
            zip_bytes=zip_bytes,
        )
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions"),
            method="POST",
            data_content=body,
            content_type=content_type,
            disable_gzip=True,
        )
        result = self._http_client.request_single_retries(request)
        # 409 means this exact version already exists — treat as success (idempotent).
        if isinstance(result, SuccessResponse) or (isinstance(result, FailedResponse) and result.status_code == 409):
            return
        result.get_success_or_raise(request)

    def transition_lifecycle(
        self,
        external_id: str,
        version: str,
        target: Literal["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"],
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions/update — advance version lifecycle state (forward-only)."""
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/update"),
            method="POST",
            body_content={
                "items": [
                    {
                        "version": version,
                        "update": {"lifecycleState": {"set": target}},
                    }
                ]
            },
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

    def set_alias(
        self,
        external_id: str,
        version: str,
        alias: Literal["ACTIVE", "PREVIEW"] | None,
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions/update — set or clear the version alias."""
        if alias is None:
            alias_update: dict = {"setNull": True}
        else:
            alias_update = {"set": alias}
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/update"),
            method="POST",
            body_content={
                "items": [
                    {
                        "version": version,
                        "update": {"alias": alias_update},
                    }
                ]
            },
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

    def list_app_versions(
        self,
        external_id: str,
        alias: str | None = None,
        limit: int = 25,
    ) -> list[AppResponse]:
        """POST /apphosting/apps/{externalId}/versions/list — list versions for one app, optionally filtered by alias."""
        body: dict = {"limit": limit}
        if alias is not None:
            body["filter"] = {"alias": alias}
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/list"),
            method="POST",
            body_content=body,
        )
        result = self._http_client.request_single_retries(request)
        if not isinstance(result, SuccessResponse):
            if isinstance(result, FailedResponse) and result.status_code in (400, 404):
                return []
            result.get_success_or_raise(request)
            return []
        data = json.loads(result.body)
        return [
            AppResponse(
                external_id=item.get("appExternalId", external_id),
                version=item["version"],
                name="",
                description=None,
                lifecycle_state=item.get("lifecycleState", "DRAFT"),
                alias=item.get("alias"),
                entrypoint=item.get("entrypoint", "index.html"),
            )
            for item in data.get("items", [])
        ]

    def retrieve_version(self, external_id: str, version: str, ignore_unknown_ids: bool = False) -> AppResponse | None:
        """Retrieve version metadata + app-level name/description in two calls."""
        version_request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/{version}"),
            method="GET",
        )
        version_result = self._http_client.request_single_retries(version_request)
        if not isinstance(version_result, SuccessResponse):
            if (
                isinstance(version_result, FailedResponse)
                and version_result.status_code in (400, 404)
                and ignore_unknown_ids
            ):
                return None
            version_result.get_success_or_raise(version_request)
            return None

        version_data = json.loads(version_result.body)

        app_request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}"),
            method="GET",
        )
        app_result = self._http_client.request_single_retries(app_request)
        app_data = json.loads(app_result.body) if isinstance(app_result, SuccessResponse) else {}

        return AppResponse(
            external_id=version_data.get("appExternalId", external_id),
            version=version_data.get("version", version),
            name=app_data.get("name", ""),
            description=app_data.get("description"),
            lifecycle_state=version_data.get("lifecycleState", "DRAFT"),
            alias=version_data.get("alias"),
            entrypoint=version_data.get("entrypoint", "index.html"),
        )

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
                        external_id=data["externalId"],
                        version="",
                        name=data.get("name", ""),
                        description=data.get("description"),
                    )
                )
            elif isinstance(result, FailedResponse) and result.status_code in (400, 404) and ignore_unknown_ids:
                continue
            else:
                result.get_success_or_raise(request)
        return results

    def iterate(self, limit: int | None = 100) -> Iterable[list[AppResponse]]:
        """POST /apphosting/versions/list — paginated list of all versions across all apps."""
        cursor: str | None = None
        page_limit = min(limit, 1000) if limit is not None else 1000
        fetched = 0
        while True:
            body: dict = {"limit": page_limit}
            if cursor:
                body["cursor"] = cursor
            request = RequestMessage(
                endpoint_url=self._url("/apphosting/versions/list"),
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
                    external_id=item["appExternalId"],
                    version=item["version"],
                    name="",
                    description=None,
                    lifecycle_state=item.get("lifecycleState", "DRAFT"),
                    alias=item.get("alias"),
                    entrypoint=item.get("entrypoint", "index.html"),
                )
                for item in data.get("items", [])
            ]
            if page_items:
                yield page_items
                fetched += len(page_items)

            cursor = data.get("nextCursor")
            if not cursor or (limit is not None and fetched >= limit):
                break

    def delete_version(self, external_id: str, versions: Sequence[AppVersionId]) -> None:
        """POST /apphosting/apps/{externalId}/versions/delete — delete specific versions of an app."""
        if not versions:
            return
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/delete"),
            method="POST",
            body_content={"items": [{"version": v.version} for v in versions]},
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

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
