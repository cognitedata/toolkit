from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceReference
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.auth import GroupAllScopedCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.datamodel import SpaceCRUD
from cognite_toolkit._cdf_tk.resource_classes import DataProductYAML


@final
class DataProductCRUD(ResourceCRUD[ExternalId, DataProductRequest, DataProductResponse]):
    folder_name = "data_products"
    resource_cls = DataProductResponse
    resource_write_cls = DataProductRequest
    kind = "DataProduct"
    yaml_cls = DataProductYAML
    dependencies = frozenset({GroupAllScopedCRUD, SpaceCRUD})
    support_drop = True
    support_update = True
    _doc_url = "Data-Products/operation/createDataProduct"

    @property
    def display_name(self) -> str:
        return "data products"

    @classmethod
    def get_id(cls, item: DataProductRequest | DataProductResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if isinstance(item, DataProductRequest):
            return item.as_id()
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "schemaSpace" in item:
            yield SpaceCRUD, SpaceReference(space=item["schemaSpace"])

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataProductRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:

        # TODO: dataproductsAcl is not yet in the SDK — return empty to skip capability verification.
        # Cannot use UnknownACL due to bug in the SDK.
        # Once available, require: CREATE, READ, UPDATE, DELETE (all four actions).
        return []

    def dump_resource(self, resource: DataProductResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        # schemaSpace is read-only (immutable after creation).
        # Always exclude it — it cannot be updated, so comparing it only creates permanent false diffs.

        if not local.get("schemaSpace"):
            dumped.pop("schemaSpace", None)

        # Strip server-set defaults for fields not explicitly in the local YAML.
        defaults: list[tuple[str, Any]] = [
            ("isGoverned", False),
            ("description", None),
            ("tags", []),
        ]
        for key, default in defaults:
            if dumped.get(key) == default and key not in local:
                dumped.pop(key)
        return dumped

    def create(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.tool.data_products.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[DataProductResponse]:
        return self.client.tool.data_products.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.tool.data_products.update(list(items))

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.data_products.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataProductResponse]:
        for items in self.client.tool.data_products.iterate(limit=None):
            yield from items
