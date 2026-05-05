from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId


class AppShared(BaseModelObject):
    """Fields shared between App Hosting request and response models."""

    external_id: str
    version: str
    name: str
    description: str | None = None
    lifecycle_state: Literal["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"] = "PUBLISHED"
    alias: Literal["ACTIVE", "PREVIEW"] | None = None
    entrypoint: str = "index.html"


class AppRequest(AppShared, UpdatableRequestResource):
    """Local representation of a Dune app version for App Hosting deployment."""

    def as_id(self) -> AppVersionId:
        return AppVersionId(external_id=self.external_id, version=self.version)

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "api"
    ) -> dict[str, Any]:
        if context == "toolkit":
            return super().dump(camel_case=camel_case, exclude_extra=exclude_extra)
        # Body for POST /apphosting/apps (ensure-app call)
        key = "externalId" if camel_case else "external_id"
        body: dict[str, Any] = {key: self.external_id, "name": self.name}
        if self.description:
            body["description"] = self.description
        return body

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return {}


class AppResponse(AppShared, ResponseResource[AppRequest]):
    """Response from App Hosting after a successful deploy."""

    @classmethod
    def request_cls(cls) -> type[AppRequest]:
        return AppRequest
