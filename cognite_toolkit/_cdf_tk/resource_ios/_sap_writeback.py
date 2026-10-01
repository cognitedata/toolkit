from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    InstancesScope,
    SAPWritebackAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sap_writeback import (
    SAPEndpointRequest,
    SAPEndpointResponse,
    SAPInstanceRequest,
    SAPInstanceResponse,
    SchemaMappingRequest,
    SchemaMappingResponse,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import SAPEndpointYAML, SAPInstanceYAML, SchemaMappingYAML


def _external_id(item: dict[str, Any], resource_name: str) -> str:
    external_id = item.get("externalId") or item.get("external_id")
    if not isinstance(external_id, str) or not external_id:
        raise ToolkitRequiredValueError(f"{resource_name} must have externalId set.")
    return external_id


@final
class SchemaMappingIO(ResourceIO[ExternalId, SchemaMappingRequest, SchemaMappingResponse, SchemaMappingYAML]):
    folder_name = "SAPwritebacks"
    resource_cls = SchemaMappingResponse
    resource_write_cls = SchemaMappingRequest
    kind = "SchemaMapping"
    yaml_cls = SchemaMappingYAML
    support_update = False
    _doc_url = "Schema-Mappings/operation/create_schema_mappings"

    @property
    def display_name(self) -> str:
        return "schema mappings"

    @classmethod
    def get_id(cls, item: SchemaMappingRequest | SchemaMappingResponse | dict[str, Any]) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=_external_id(item, "Schema mapping"))
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SchemaMappingRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | InstancesScope):
            yield SAPWritebackAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: SchemaMappingYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    def dump_resource(self, resource: SchemaMappingResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump()

    def create(self, items: Sequence[SchemaMappingRequest]) -> list[SchemaMappingResponse]:
        return self.client.sap_writeback.mappings.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[SchemaMappingResponse]:
        if not ids:
            return []
        return self.client.sap_writeback.mappings.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.sap_writeback.mappings.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SchemaMappingResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (item for page in self.client.sap_writeback.mappings.iterate(limit=None) for item in page)


@final
class SAPInstanceIO(ResourceIO[ExternalId, SAPInstanceRequest, SAPInstanceResponse, SAPInstanceYAML]):
    folder_name = "SAPwritebacks"
    resource_cls = SAPInstanceResponse
    resource_write_cls = SAPInstanceRequest
    kind = "SAPInstance"
    yaml_cls = SAPInstanceYAML
    support_update = False
    _doc_url = "SAP-Instances/operation/create_instances"

    @property
    def display_name(self) -> str:
        return "SAP instances"

    @classmethod
    def get_id(cls, item: SAPInstanceRequest | SAPInstanceResponse | dict[str, Any]) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=_external_id(item, "SAP instance"))
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SAPInstanceRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | InstancesScope):
            yield SAPWritebackAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: SAPInstanceYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    def dump_resource(self, resource: SAPInstanceResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        # We will always redeploy SAP instances in case the password has changed.
        return resource.model_dump(exclude={"created_time", "last_updated_time"}, by_alias=True)

    def sensitive_strings(self, item: SAPInstanceRequest) -> Iterable[str]:
        yield item.password

    def create(self, items: Sequence[SAPInstanceRequest]) -> list[SAPInstanceResponse]:
        return self.client.sap_writeback.instances.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[SAPInstanceResponse]:
        if not ids:
            return []
        return self.client.sap_writeback.instances.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.sap_writeback.instances.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SAPInstanceResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (item for page in self.client.sap_writeback.instances.iterate(limit=None) for item in page)


@final
class SAPEndpointIO(ResourceIO[ExternalId, SAPEndpointRequest, SAPEndpointResponse, SAPEndpointYAML]):
    folder_name = "SAPwritebacks"
    resource_cls = SAPEndpointResponse
    resource_write_cls = SAPEndpointRequest
    kind = "SAPEndpoint"
    yaml_cls = SAPEndpointYAML
    dependencies = frozenset({SAPInstanceIO, SchemaMappingIO})
    support_update = False
    _doc_url = "SAP-Endpoints/operation/create_endpoints"

    @property
    def display_name(self) -> str:
        return "SAP endpoints"

    @classmethod
    def get_id(cls, item: SAPEndpointRequest | SAPEndpointResponse | dict[str, Any]) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=_external_id(item, "SAP endpoint"))
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SAPEndpointRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | InstancesScope):
            yield SAPWritebackAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: SAPEndpointYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield SAPInstanceIO, ExternalId(external_id=resource.instance_id)
        if resource.mapping_id:
            yield SchemaMappingIO, ExternalId(external_id=resource.mapping_id)

    def dump_resource(self, resource: SAPEndpointResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump()

    def create(self, items: Sequence[SAPEndpointRequest]) -> list[SAPEndpointResponse]:
        return self.client.sap_writeback.endpoints.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[SAPEndpointResponse]:
        if not ids:
            return []
        return self.client.sap_writeback.endpoints.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.sap_writeback.endpoints.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SAPEndpointResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (item for page in self.client.sap_writeback.endpoints.iterate(limit=None) for item in page)
