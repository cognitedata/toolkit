from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId, ViewNoVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AclType, AllScope, DataProductScope, ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import DataProductsAcl
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._datamodel import SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.utils.acl_helper import as_read_create_update_delete_actions
from cognite_toolkit._cdf_tk.yaml_classes import DataProductYAML


@final
class DataProductIO(ResourceIO[ExternalId, DataProductRequest, DataProductResponse, DataProductYAML]):
    folder_name = "data_products"
    resource_cls = DataProductResponse
    resource_write_cls = DataProductRequest
    kind = "DataProduct"
    yaml_cls = DataProductYAML
    dependencies = frozenset({SpaceIO, ViewIO})
    support_drop = True
    support_update = True
    _doc_url = "Data-Products/operation/createDataProduct"

    @property
    def display_name(self) -> str:
        return "data products"

    @classmethod
    def get_id(cls, item: DataProductRequest | DataProductResponse | ExternalId | dict) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if isinstance(item, DataProductRequest):
            return item.as_id()
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    @classmethod
    def get_dependencies(cls, resource: DataProductYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.schema_space:
            yield SpaceIO, SpaceId(space=resource.schema_space)
        for scope in resource.instance_scope or []:
            yield ViewIO, ViewNoVersionId(space=scope.source.space, external_id=scope.source.external_id)
            for space in [scope.source.space, *scope.read, *scope.upsert, *scope.delete]:
                yield SpaceIO, SpaceId(space=space)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[DataProductRequest]) -> ScopeDefinition | None:
        return DataProductScope(external_ids=sorted({item.external_id for item in items}))

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataProductScope):
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
                dumped.pop(key, None)
        # Toolkit only manages an instanceScope that the YAML declares.
        if "instanceScope" not in local:
            dumped.pop("instanceScope", None)
        elif dumped.get("instanceScope"):
            self._strip_scope_defaults(dumped["instanceScope"], local["instanceScope"])
        elif local["instanceScope"] == []:
            # The API cannot tell an empty scope from an unset one, so it returns no instanceScope for both.
            dumped["instanceScope"] = []
        else:
            dumped.pop("instanceScope", None)
        return dumped

    @staticmethod
    def _strip_scope_defaults(dumped_scope: list[dict[str, Any]], local_scope: list[dict[str, Any]] | None) -> None:
        """Remove source type and empty upsert/delete lists that the local entry for the same view leaves out."""
        local_by_view = {
            (entry.get("source", {}).get("space"), entry.get("source", {}).get("externalId")): entry
            for entry in local_scope or []
        }
        for entry in dumped_scope:
            local_entry = local_by_view.get((entry["source"]["space"], entry["source"]["externalId"]))
            if local_entry is None:
                continue
            if "type" not in local_entry["source"]:
                entry["source"].pop("type", None)
            for key in ("upsert", "delete"):
                if entry.get(key) == [] and key not in local_entry:
                    entry.pop(key)

    def create(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.data_products.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[DataProductResponse]:
        return self.client.data_products.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.data_products.update(list(items))

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.data_products.delete(id_list)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataProductResponse]:
        for items in self.client.data_products.iterate(limit=None):
            yield from items
