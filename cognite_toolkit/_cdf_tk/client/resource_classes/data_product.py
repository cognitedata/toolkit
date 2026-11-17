from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, ViewNoVersionId


class DataProductInstanceScope(BaseModelObject):
    """The spaces whose instances are in scope through one view of a data product."""

    source: ViewNoVersionId
    read: list[str]
    upsert: list[str] | None = None
    delete: list[str] | None = None


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
    instance_scope: list[DataProductInstanceScope] | None = None

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = super().as_update(mode)
        update = update_item.get("update", {})
        # schemaSpace is immutable after creation — the API rejects it in updates.
        update.pop("schemaSpace", None)
        # The instance scope patch only supports set, an empty list clears it.
        if update.get("instanceScope") == {"setNull": True}:
            update["instanceScope"] = {"set": []}
        return update_item


class DataProductResponse(DataProduct, ResponseResource[DataProductRequest]):
    """Response resource for data products."""

    schema_space: str
    is_governed: bool
    tags: list[str]
    domains: list[str]
    description: str | None = None
    instance_scope: list[DataProductInstanceScope] | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DataProductRequest]:
        return DataProductRequest
