from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class AppShared(BaseModelObject):
    """Fields shared between App Hosting request and response models."""

    app_external_id: str
    version_tag: str
    name: str
    description: str | None = None
    published: bool = False
    entry_path: str = "index.html"


class AppRequest(AppShared, UpdatableRequestResource):
    """Local representation of a Dune app version for App Hosting deployment."""

    @property
    def external_id(self) -> str:
        return self.app_external_id

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.app_external_id)

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "api"
    ) -> dict[str, Any]:
        if context == "toolkit":
            return super().dump(camel_case=camel_case, exclude_extra=exclude_extra)
        # Body for POST /apphosting/apps (ensure-app call)
        key = "externalId" if camel_case else "external_id"
        return {
            key: self.app_external_id,
            "name": self.name,
            "description": self.description or "",
        }

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return {}


class AppResponse(AppShared, ResponseResource[AppRequest]):
    """Response from App Hosting after a successful deploy."""

    lifecycle_state: str | None = None
    alias: str | None = None

    @classmethod
    def request_cls(cls) -> type[AppRequest]:
        return AppRequest
