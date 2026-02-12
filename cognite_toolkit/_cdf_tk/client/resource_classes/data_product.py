from __future__ import annotations

from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .data_product_version import DataProductVersionBase, DataProductVersionResponse
from .identifiers import ExternalId


class SpaceReference(BaseModelObject):
    """Reference to a CDF space."""

    space: str


class OwnerGroup(BaseModelObject):
    """Ownership information for a data product."""

    members: list[str]


class DataProduct(BaseModelObject):
    """Base class for data product with common fields."""

    external_id: str
    name: str
    instance_read_spaces: list[SpaceReference]
    governance_status: str  # "governed" | "ungoverned"
    description: str | None = None
    source_domains: list[str] | None = None
    data_model_spaces: list[SpaceReference] | None = None
    instance_write_space: SpaceReference | None = None
    owner_group: OwnerGroup | None = None


class DataProductRequest(DataProduct, UpdatableRequestResource):
    """Request resource for creating/updating data products."""

    initial_version: DataProductVersionBase

    container_fields: ClassVar[frozenset[str]] = frozenset(
        {
            "instance_read_spaces",
            "source_domains",
            "data_model_spaces",
        }
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DataProductResponse(DataProduct, ResponseResource[DataProductRequest]):
    """Response resource for data products."""

    versions: list[DataProductVersionResponse] | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DataProductRequest:
        # Extract the latest version or first version as initial_version
        initial = DataProductVersionBase(
            data_model=self.versions[0].data_model if self.versions else None,  # type: ignore[arg-type]
            version_status=self.versions[0].version_status if self.versions else None,
        )
        return DataProductRequest(
            external_id=self.external_id,
            name=self.name,
            instance_read_spaces=self.instance_read_spaces,
            governance_status=self.governance_status,
            description=self.description,
            source_domains=self.source_domains,
            data_model_spaces=self.data_model_spaces,
            instance_write_space=self.instance_write_space,
            owner_group=self.owner_group,
            initial_version=initial,
        )
