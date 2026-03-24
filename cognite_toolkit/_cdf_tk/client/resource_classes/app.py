from typing import Any, Literal

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

DUNE_APPS_DIRECTORY = "/dune-apps/"


class AppShared(BaseModelObject):
    """Fields shared between app write/read models (under /dune-apps/)."""

    app_external_id: str = Field(description="Logical app id; directory name in the module.")
    version: str
    name: str
    description: str | None = None
    published: bool | None = True
    data_set_id: int | None = None
    cognite_toolkit_app_hash: str | None = Field(None, alias="cdf-toolkit-app-hash")


class AppRequest(AppShared, UpdatableRequestResource):
    """Create/update body for Files API init; zip is uploaded separately to uploadUrl."""

    @property
    def external_id(self) -> str:
        return f"{self.app_external_id}-{self.version}"

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "api"
    ) -> dict[str, Any]:
        if context == "toolkit":
            dumped = super().dump(camel_case=camel_case, exclude_extra=exclude_extra)
            dumped.pop("externalId", None)
            dumped.pop("external_id", None)
            return dumped
        feid = self.external_id
        return {
            "externalId" if camel_case else "external_id": feid,
            "name": f"{feid}.zip",
            "dataSetId" if camel_case else "data_set_id": self.data_set_id,
            "directory": DUNE_APPS_DIRECTORY,
            "metadata": self._as_metadata(),
        }

    def _as_metadata(self) -> dict[str, str]:
        metadata: dict[str, str] = {
            "published": str(bool(self.published)).lower(),
            "name": self.name,
            "description": self.description or "",
            "externalId": self.external_id,
            "version": self.version,
            "appExternalId": self.app_external_id,
        }
        if self.cognite_toolkit_app_hash:
            metadata["cdf-toolkit-app-hash"] = self.cognite_toolkit_app_hash
        return metadata

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_data: dict[str, Any] = {
            "metadata": {"set": self._as_metadata()},
            "directory": {"set": DUNE_APPS_DIRECTORY},
        }
        if self.data_set_id is not None:
            update_data["dataSetId"] = {"set": self.data_set_id}
        elif mode == "replace":
            update_data["dataSetId"] = {"setNull": True}

        return {
            "externalId": self.external_id,
            "update": update_data,
        }


class AppResponse(AppShared, ResponseResource[AppRequest]):
    """File metadata for a Dune app after create/retrieve (metadata merged to top level)."""

    external_id: str
    id: int
    created_time: int
    last_updated_time: int
    uploaded_time: int | None = None
    uploaded: bool
    upload_url: str | None = None

    @classmethod
    def request_cls(cls) -> type[AppRequest]:
        return AppRequest

    @model_validator(mode="before")
    @classmethod
    def move_metadata(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "metadata" not in values:
            return values
        values_copy = values.copy()
        metadata = values_copy.pop("metadata")
        values_copy.update(metadata)
        return values_copy
