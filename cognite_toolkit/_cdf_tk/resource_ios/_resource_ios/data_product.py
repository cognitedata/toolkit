from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AclType, AllScope, ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import DataProductsAcl
from cognite_toolkit._cdf_tk.resource_ios._base_cruds import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.auth import GroupAllScopedCRUD
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel import SpaceCRUD
from cognite_toolkit._cdf_tk.utils.acl_helper import as_read_create_update_delete_actions
from cognite_toolkit._cdf_tk.yaml_classes import DataProductYAML


@final
class DataProductIO(ResourceIO[ExternalId, DataProductRequest, DataProductResponse]):
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
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        if "schemaSpace" in item:
            yield SpaceCRUD, SpaceId(space=item["schemaSpace"])

    @classmethod
    def get_dependencies(cls, resource: DataProductYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.schema_space:
            yield SpaceCRUD, SpaceId(space=resource.schema_space)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[DataProductRequest]) -> ScopeDefinition | None:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield DataProductsAcl(
                actions=as_read_create_update_delete_actions(actions),
                scope=scope,
            )

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
