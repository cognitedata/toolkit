from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    InstancesScope,
    SAPWritebackAcl,
    SAPWritebackRequestsAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sap_writeback import (
    SAPEndpointRequest,
    SAPEndpointResponse,
    SAPInstanceRequest,
    SAPInstanceResponse,
    SchemaMappingRequest,
    SchemaMappingResponse,
    WritebackRequestRequest,
    WritebackRequestResponse,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import (
    SAPEndpointYAML,
    SAPInstanceYAML,
    SchemaMappingYAML,
    WritebackRequestYAML,
)

_RESPONSE_ONLY_FIELDS = ("createdTime", "lastUpdatedTime")


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
    def get_id(cls, item: SchemaMappingRequest | SchemaMappingResponse | dict) -> ExternalId:
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
    def get_id(cls, item: SAPInstanceRequest | SAPInstanceResponse | dict) -> ExternalId:
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
        # The API does not return the password. Keep the local value so deploy can compare the instance
        # without treating every password as a change.
        dumped: dict[str, Any] = {
            "externalId": resource.external_id,
            "gatewayUrl": resource.gateway_url,
            "client": resource.client,
            "username": resource.username,
        }
        local = local or {}
        if "password" in local:
            dumped["password"] = local["password"]
        return dumped

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
    def get_id(cls, item: SAPEndpointRequest | SAPEndpointResponse | dict) -> ExternalId:
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


@final
class WritebackRequestIO(
    ResourceIO[WritebackRequestId, WritebackRequestRequest, WritebackRequestResponse, WritebackRequestYAML]
):
    folder_name = "SAPwritebacks"
    resource_cls = WritebackRequestResponse
    resource_write_cls = WritebackRequestRequest
    kind = "WritebackRequest"
    yaml_cls = WritebackRequestYAML
    dependencies = frozenset({SAPEndpointIO})
    support_update = False
    support_drop = False
    _doc_url = "Writeback-Requests/operation/create_writeback_requests"

    @property
    def display_name(self) -> str:
        return "writeback requests"

    @classmethod
    def get_id(cls, item: WritebackRequestRequest | WritebackRequestResponse | dict) -> WritebackRequestId:
        if isinstance(item, dict):
            request_id = item.get("requestId") or item.get("request_id")
        elif isinstance(item, WritebackRequestResponse):
            return item.as_id()
        else:
            extra = item.model_extra or {}
            request_id = extra.get("requestId") or extra.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise ToolkitRequiredValueError("Writeback request must have requestId set.")
        return WritebackRequestId(request_id=request_id)

    @classmethod
    def dump_id(cls, id: WritebackRequestId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: WritebackRequestId) -> str:
        return sanitize_filename(id.request_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[WritebackRequestRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if not isinstance(scope, AllScope | InstancesScope):
            return
        acl_actions: list[Literal["WRITE", "LIST"]] = []
        if "READ" in actions:
            acl_actions.append("LIST")
        if "WRITE" in actions:
            acl_actions.append("WRITE")
        if acl_actions:
            yield SAPWritebackRequestsAcl(actions=acl_actions, scope=scope)

    @classmethod
    def get_dependencies(cls, resource: WritebackRequestYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield SAPEndpointIO, ExternalId(external_id=resource.endpoint_id)

    def dump_resource(self, resource: WritebackRequestResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        # Responses omit endpointId and the original item shape. Compare against the local file when it exists
        # so an existing request is not treated as changed.
        if local:
            return dict(local)
        dumped = resource.dump()
        for field in (*_RESPONSE_ONLY_FIELDS, "status"):
            dumped.pop(field, None)
        return dumped

    def create(self, items: Sequence[WritebackRequestRequest]) -> list[WritebackRequestResponse]:
        # requestId is a Toolkit identifier. The create API assigns its own request ID and rejects extras.
        cleaned = [WritebackRequestRequest(endpoint_id=item.endpoint_id, request=item.request) for item in items]
        return self.client.sap_writeback.create(cleaned)

    def retrieve(self, ids: Sequence[WritebackRequestId]) -> list[WritebackRequestResponse]:
        if not ids:
            return []
        return self.client.sap_writeback.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[WritebackRequestId]) -> int:
        raise ToolkitNotSupported("Writeback requests cannot be deleted.")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[WritebackRequestResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (item for page in self.client.sap_writeback.iterate(limit=None) for item in page)
