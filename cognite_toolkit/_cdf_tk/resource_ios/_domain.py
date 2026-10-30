from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.domain import DomainRequest, DomainResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AclType, AllScope, DataDomainScope, ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import DataDomainsAcl
from cognite_toolkit._cdf_tk.resource_ios._auth import GroupAllScopedIO
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.acl_helper import as_read_create_update_delete_actions
from cognite_toolkit._cdf_tk.yaml_classes import DomainYAML


@final
class DomainIO(ResourceIO[ExternalId, DomainRequest, DomainResponse, DomainYAML]):
    folder_name = "domains"
    resource_cls = DomainResponse
    resource_write_cls = DomainRequest
    kind = "Domain"
    yaml_cls = DomainYAML
    dependencies = frozenset({GroupAllScopedIO})
    support_drop = True
    support_update = True
    _doc_url = "Data-domains/operation/createDataDomain"

    @property
    def display_name(self) -> str:
        return "domains"

    @classmethod
    def get_id(cls, item: DomainRequest | DomainResponse | ExternalId | dict) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependencies(cls, resource: DomainYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    @classmethod
    def get_minimum_scope(cls, items: Sequence[DomainRequest]) -> ScopeDefinition:
        return DataDomainScope(external_ids=sorted({item.external_id for item in items}))

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataDomainScope):
            yield DataDomainsAcl(
                actions=as_read_create_update_delete_actions(actions),
                scope=scope,
            )

    def dump_resource(self, resource: DomainResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if dumped.get("description") is None and "description" not in local:
            dumped.pop("description", None)
        return dumped

    def create(self, items: Sequence[DomainRequest]) -> list[DomainResponse]:
        return self.client.domains.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[DomainResponse]:
        return self.client.domains.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DomainRequest]) -> list[DomainResponse]:
        return self.client.domains.update(list(items))

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.domains.delete(id_list)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DomainResponse]:
        for items in self.client.domains.iterate(limit=None):
            yield from items
