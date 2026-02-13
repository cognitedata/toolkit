from __future__ import annotations

from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class DataProduct(BaseModelObject):
    """Base class for data product with common fields."""

    external_id: str
    name: str
    is_governed: bool = False
    description: str | None = None
    schema_space: str | None = None
    tags: list[str] | None = None


class DataProductRequest(DataProduct, UpdatableRequestResource):
    """Request resource for creating/updating data products."""

    container_fields: ClassVar[frozenset[str]] = frozenset(
        {
            "tags",
        }
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DataProductResponse(DataProduct, ResponseResource[DataProductRequest]):
    """Response resource for data products."""

    domains: list[str] | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DataProductRequest:
        return DataProductRequest(
            external_id=self.external_id,
            name=self.name,
            is_governed=self.is_governed,
            description=self.description,
            schema_space=self.schema_space,
            tags=self.tags,
        )
