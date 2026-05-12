"""AppsAPI: Custom apps deployed via the CDF App Hosting API."""

import json
from collections.abc import Iterable, Sequence
from pathlib import Path

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.http_client._exception import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse


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
        zip_path: Path,
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions — multipart upload of the zipped app."""
        result = self._http_client.request_raw_retries(
            method="POST",
            url=self._url(f"/apphosting/apps/{external_id}/versions"),
            files={"file": ("app.zip", zip_path, "application/zip")},
            data={"version": version, "entryPath": entrypoint},
            add_auth=True,
        )
        # 409 means this exact version already exists — treat as success (idempotent).
        if isinstance(result, SuccessResponse) or (isinstance(result, FailedResponse) and result.status_code == 409):
            return
        raise ToolkitAPIError(message=result.body, code=result.status_code)

    def update_version(self, external_id: str, version: str, update: dict) -> None:
        """POST /apphosting/apps/{externalId}/versions/update — apply one or more field updates to a version."""
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/update"),
            method="POST",
            body_content={"items": [{"version": version, "update": update}]},
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

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
