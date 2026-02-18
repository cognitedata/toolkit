from typing import Any, ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import DataProductVersionId, SemanticVersion


class ViewInstanceSpaces(BaseModelObject):
    read: list[str] = Field(default_factory=list)
    write: list[str] = Field(default_factory=list)


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
        raise NotImplementedError("Data product version updates use a custom format via the CRUD layer.")


class DataProductVersionResponse(DataProductVersion, ResponseResource[DataProductVersionRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DataProductVersionRequest:
        data = self.dump(camel_case=False)
        data["data_product_external_id"] = self.data_product_external_id
        return DataProductVersionRequest.model_validate(data, extra="ignore")
