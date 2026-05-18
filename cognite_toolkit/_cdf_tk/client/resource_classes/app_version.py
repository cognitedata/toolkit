from typing import Literal, get_args

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import App

LifecycleState = Literal["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"]
LIFECYCLE_ORDER: tuple[str, ...] = get_args(LifecycleState)


class AppVersion(BaseModelObject):
    version: str
    lifecycle_state: LifecycleState = "PUBLISHED"
    alias: Literal["ACTIVE", "PREVIEW"] | None = None
    entrypoint: str = "index.html"


class AppVersionRequest(App, AppVersion, RequestResource):
    """Toolkit write class — the union of App (externalId/name/description) and AppVersion
    (version/lifecycleState/alias/entrypoint) fields, matching the single-YAML user experience.

    The App Hosting API splits these across two endpoints: POST /apphosting/apps and
    POST /apphosting/apps/{id}/versions. AppIO._deploy splits this object into both calls.
    AppVersionResponse uses app_external_id (not external_id) and omits name/description because
    the versions API wire format differs from the user-facing YAML representation.
    """

    def as_id(self) -> AppVersionId:
        return AppVersionId(app_external_id=self.external_id, version=self.version)


class AppVersionResponse(AppVersion, ResponseResource[AppVersionRequest]):
    """Response from the App Hosting versions API (GET/POST /apphosting/apps/{id}/versions/...).

    Uses app_external_id (not external_id) because the wire format returns `appExternalId` to
    refer to the parent app's ID. App versions themselves do not have an unique `externalId` field.
    """

    app_external_id: str

    @classmethod
    def request_cls(cls) -> type[AppVersionRequest]:
        return AppVersionRequest
