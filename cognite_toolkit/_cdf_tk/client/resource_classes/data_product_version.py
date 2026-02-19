from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .identifiers import DataProductVersionId, SemanticVersion

SpaceId = Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN, max_length=43)]


class ViewInstanceSpaces(BaseModelObject):
    read: list[SpaceId] = Field(default_factory=list)
    write: list[SpaceId] = Field(default_factory=list)


class DataProductVersionView(BaseModelObject):
    external_id: str
    instance_spaces: ViewInstanceSpaces = Field(default_factory=ViewInstanceSpaces)


class DataProductVersionDataModel(BaseModelObject):
    external_id: str
    version: str
    views: list[DataProductVersionView] = Field(default_factory=list)


class DataProductVersionTerms(BaseModelObject):
    usage: str | None = None
    limitations: str | None = None


class DataProductVersion(BaseModelObject):
    data_product_external_id: str = Field(exclude=True)
    version: SemanticVersion
    data_model: DataProductVersionDataModel
    status: Literal["draft", "published", "deprecated"] = "draft"
    description: str | None = None
    terms: DataProductVersionTerms | None = None

    def as_id(self) -> DataProductVersionId:
        return DataProductVersionId(
            data_product_external_id=self.data_product_external_id,
            version=self.version,
        )


class DataProductVersionRequest(DataProductVersion, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset()

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        # The versions update API uses nested {set}/{setNull}/{modify} operators
        # instead of a flat body, so we must build the payload manually.
        update_item: dict[str, Any] = {"version": self.version}
        update: dict[str, Any] = {}
        exclude_unset = mode == "patch"
        dumped = self.model_dump(mode="json", by_alias=True, exclude_unset=exclude_unset)

        for key in ("status", "description"):
            if key not in dumped:
                continue
            if dumped[key] is None:
                update[key] = {"setNull": True}
            else:
                update[key] = {"set": dumped[key]}

        if "terms" in dumped and dumped["terms"] is not None:
            terms_modify: dict[str, Any] = {}
            for sub_key in ("usage", "limitations"):
                if sub_key in dumped["terms"]:
                    val = dumped["terms"][sub_key]
                    terms_modify[sub_key] = {"setNull": True} if val is None else {"set": val}
            if terms_modify:
                update["terms"] = {"modify": terms_modify}

        if "dataModel" in dumped and dumped["dataModel"] is not None:
            views = dumped["dataModel"].get("views")
            if views is not None:
                update["dataModel"] = {"modify": {"views": {"set": views}}}

        update_item["update"] = update
        return update_item


class DataProductVersionResponse(DataProductVersion, ResponseResource[DataProductVersionRequest]):
    data_product_external_id: str = Field(default="", exclude=True)
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DataProductVersionRequest:
        data = self.dump(camel_case=False)
        data["data_product_external_id"] = self.data_product_external_id
        return DataProductVersionRequest.model_validate(data, extra="ignore")
