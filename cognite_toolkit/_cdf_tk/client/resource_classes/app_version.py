from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId


class AppVersion(BaseModelObject):
    version: str
    lifecycle_state: Literal["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"] = "PUBLISHED"
    alias: Literal["ACTIVE", "PREVIEW"] | None = None
    entrypoint: str = "index.html"


class AppVersionRequest(AppVersion, RequestResource):
    """Write class for POST /apphosting/apps/{externalId}/versions."""

    app_external_id: str

    def as_id(self) -> AppVersionId:
        return AppVersionId(app_external_id=self.app_external_id, version=self.version)


class AppVersionResponse(AppVersion, ResponseResource[AppVersionRequest]):
    """Response from the App Hosting versions API (GET/POST /apphosting/apps/{id}/versions/...).

    Uses app_external_id (not external_id) because the wire format returns `appExternalId` to
    refer to the parent app's ID. App versions themselves do not have an unique `externalId` field.
    """

    app_external_id: str

    @classmethod
    def request_cls(cls) -> type[AppVersionRequest]:
        return AppVersionRequest
