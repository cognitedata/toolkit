from typing import Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ViewNoVersionUntypedId


class DataProductRole(BaseModelObject):
    """A role of a data product. View access is managed separately through access filters."""

    external_id: str
    name: str | None = None
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DataProductRoleRequest(DataProductRole, UpdatableRequestResource):
    pass


class DataProductRoleResponse(DataProductRole, ResponseResource[DataProductRoleRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DataProductRoleRequest]:
        return DataProductRoleRequest


class DataProductRoleAccessCondition(BaseModelObject):
    """A top-level ``and`` of ``equals`` and ``in`` filters. An empty ``and`` grants access to all instances."""

    and_: list[dict[str, JsonValue]] = Field(alias="and")
    resources: list[Literal["FILES", "DATA_POINTS"]] | None = None


class DataProductRoleAccessFilterRequest(BaseModelObject):
    """Grants a role read access, and optionally upsert and delete access, to one view.

    The view is identified by space and external ID, the server resolves the version.
    """

    view: ViewNoVersionUntypedId
    read: DataProductRoleAccessCondition
    upsert: DataProductRoleAccessCondition | None = None
    delete: DataProductRoleAccessCondition | None = None


class DataProductRoleAccessFilter(BaseModelObject):
    role_external_id: str
    view: ViewNoVersionUntypedId
    read: DataProductRoleAccessCondition | None = None
    write: DataProductRoleAccessCondition | None = None
    upsert: DataProductRoleAccessCondition | None = None
    delete: DataProductRoleAccessCondition | None = None
    created_time: int
    last_updated_time: int
