"""AppVersionsAPI: Version management for custom apps via the CDF App Hosting API."""

import json
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage, ToolkitAPIError
from cognite_toolkit._cdf_tk.client.http_client._data_classes import FailedResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app_version import AppVersionResponse


class AppVersionsAPI:
    """Client for the CDF App Hosting Versions API (POST /apphosting/apps/{externalId}/versions/...)."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _url(self, path: str) -> str:
        return self._http_client.config.create_api_url(path)

    def upload(
        self,
        external_id: str,
        version: str,
        entrypoint: str,
        zip_bytes: bytes,
    ) -> None:
        """POST /apphosting/apps/{externalId}/versions — multipart/form-data upload of the zipped app."""
        result = self._http_client.request_multipart_retries(
            url=self._url(f"/apphosting/apps/{external_id}/versions"),
            files={"file": ("app.zip", zip_bytes, "application/zip")},
            form_fields={"version": version, "entryPath": entrypoint},
        )
        if isinstance(result, FailedResponse):
            raise ToolkitAPIError(message=result.body, code=result.status_code)

    def update(self, external_id: str, version: str, patch: dict) -> None:
        """POST /apphosting/apps/{externalId}/versions/update — apply a lifecycle/alias patch to a version."""
        request = RequestMessage(
            endpoint_url=self._url(f"/apphosting/apps/{external_id}/versions/update"),
            method="POST",
            body_content={"items": [{"version": version, "update": patch}]},
        )
        self._http_client.request_single_retries(request).get_success_or_raise(request)

    def retrieve(self, items: Sequence[AppVersionId], ignore_unknown_ids: bool = False) -> list[AppVersionResponse]:
        """GET /apphosting/apps/{externalId}/versions/{version} — retrieve version metadata."""
        results: list[AppVersionResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._url(f"/apphosting/apps/{item.app_external_id}/versions/{item.version}"),
                method="GET",
            )
            result = self._http_client.request_single_retries(request)
            if not isinstance(result, SuccessResponse):
                if isinstance(result, FailedResponse) and result.status_code in (400, 404) and ignore_unknown_ids:
                    # As of 2026-05-19, the apphosting service returns 400 (not 404) for unknown versions.
                    continue
                result.get_success_or_raise(request)
                continue
            results.append(AppVersionResponse.model_validate_json(result.body))
        return results

    def iterate(self, limit: int | None = 100) -> Iterable[list[AppVersionResponse]]:
        """POST /apphosting/versions/list — paginated list of all versions across all apps."""
        cursor: str | None = None
        fetched = 0
        while True:
            page_limit = min(limit - fetched, 1000) if limit is not None else 1000
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
            page_items = [AppVersionResponse.model_validate(item) for item in data.get("items", [])]
            if page_items:
                yield page_items
                fetched += len(page_items)

            cursor = data.get("nextCursor")
            if not cursor or (limit is not None and fetched >= limit):
                break

    def delete(self, versions: Sequence[AppVersionId]) -> None:
        """POST /apphosting/apps/{externalId}/versions/delete — delete specific versions, grouped by app."""
        by_app: dict[str, list[AppVersionId]] = {}
        for version_id in versions:
            by_app.setdefault(version_id.app_external_id, []).append(version_id)
        for app_external_id, app_versions in by_app.items():
            request = RequestMessage(
                endpoint_url=self._url(f"/apphosting/apps/{app_external_id}/versions/delete"),
                method="POST",
                body_content={"items": [{"version": v.version} for v in app_versions]},
            )
            self._http_client.request_single_retries(request).get_success_or_raise(request)
