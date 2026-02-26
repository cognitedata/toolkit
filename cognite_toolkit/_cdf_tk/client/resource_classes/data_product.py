from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId


class DataProduct(BaseModelObject):
    """Represents a data product in CDF."""

    external_id: str
    name: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DataProductRequest(DataProduct, UpdatableRequestResource):
    """Request resource for creating/updating data products."""

    container_fields: ClassVar[frozenset[str]] = frozenset({"tags"})

    description: str | None = None
    schema_space: str | None = None
    is_governed: bool = False
    tags: list[str] | None = None

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = super().as_update(mode)
        # schemaSpace is immutable after creation — the API rejects it in updates.
        update_item.get("update", {}).pop("schemaSpace", None)
        return update_item


class DataProductResponse(DataProduct, ResponseResource[DataProductRequest]):
    """Response resource for data products."""

    schema_space: str
    is_governed: bool
    tags: list[str]
    domains: list[str]
    description: str | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DataProductRequest]:
        return DataProductRequest
