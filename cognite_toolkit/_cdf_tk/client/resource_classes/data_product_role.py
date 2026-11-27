from typing import Literal

from pydantic import ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import DataProductRoleId, ViewNoVersionUntypedId


class DataProductRole(BaseModelObject):
    """A role of a data product. View access is managed separately through access filters."""

    # data_product_external_id is a path parameter, not part of the request body.
    data_product_external_id: str = Field(exclude=True)
    external_id: str
    name: str | None = None
    description: str | None = None

    def as_id(self) -> DataProductRoleId:
        return DataProductRoleId(data_product_external_id=self.data_product_external_id, external_id=self.external_id)


class DataProductRoleRequest(DataProductRole, UpdatableRequestResource):
    pass


class DataProductRoleResponse(DataProductRole, ResponseResource[DataProductRoleRequest]):
    # data_product_external_id is a path parameter, not returned in the API response body.
    data_product_external_id: str = Field(default="", exclude=True)
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DataProductRoleRequest]:
        return DataProductRoleRequest

    def as_request_resource(self) -> DataProductRoleRequest:
        dumped = self.dump()
        dumped["dataProductExternalId"] = self.data_product_external_id
        return DataProductRoleRequest.model_validate(dumped, extra="ignore")


class DataProductRoleAccessMutationCondition(BaseModelObject):
    """A top-level ``and`` of ``equals`` and ``in`` filters for upsert and delete, without resource access."""

    model_config = ConfigDict(extra="forbid")

    and_: list[dict[str, JsonValue]] = Field(alias="and")


class DataProductRoleAccessCondition(BaseModelObject):
    """A top-level ``and`` of ``equals`` and ``in`` filters. An empty ``and`` grants access to all instances."""

    and_: list[dict[str, JsonValue]] = Field(alias="and")
    resources: list[Literal["FILES", "DATA_POINTS"] | str] | None = None


class DataProductRoleAccessFilterRequest(BaseModelObject):
    """Grants a role read access, and optionally write, upsert and delete access, to one view.

    The view is identified by space and external ID, the server resolves the version.
    """

    view: ViewNoVersionUntypedId
    read: DataProductRoleAccessCondition
    write: DataProductRoleAccessCondition | None = None
    upsert: DataProductRoleAccessMutationCondition | None = None
    delete: DataProductRoleAccessMutationCondition | None = None


class DataProductRoleAccessFilter(BaseModelObject):
    role_external_id: str
    view: ViewNoVersionUntypedId
    read: DataProductRoleAccessCondition | None = None
    write: DataProductRoleAccessCondition | None = None
    upsert: DataProductRoleAccessMutationCondition | None = None
    delete: DataProductRoleAccessMutationCondition | None = None
    created_time: int
    last_updated_time: int
