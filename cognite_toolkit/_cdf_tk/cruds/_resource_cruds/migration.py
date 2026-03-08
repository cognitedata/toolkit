from collections.abc import Hashable, Iterable, Sequence, Sized
from typing import Any, Literal, final

from cognite.client import data_modeling as dm
from cognite.client.data_classes import capabilities as cap

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    Acl,
    AllScope,
    DataModelInstancesAcl,
    ScopeDefinition,
    SpaceIDScope,
)
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    RESOURCE_MAPPING_VIEW_ID,
    ResourceViewMappingRequest,
    ResourceViewMappingResponse,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.utils import in_dict, sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import ResourceViewMappingYAML

from .datamodel import SpaceCRUD, ViewCRUD


@final
class ResourceViewMappingCRUD(ResourceCRUD[ExternalId, ResourceViewMappingRequest, ResourceViewMappingResponse]):
    folder_name = "migration"
    resource_cls = ResourceViewMappingResponse
    resource_write_cls = ResourceViewMappingRequest
    kind = "ResourceViewMapping"
    dependencies = frozenset({SpaceCRUD, ViewCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"
    yaml_cls = ResourceViewMappingYAML

    @property
    def display_name(self) -> str:
        return "resource view mapping"

    @classmethod
    def get_id(cls, item: ResourceViewMappingRequest | ResourceViewMappingResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ResourceViewMappingRequest] | None, read_only: bool
    ) -> cap.Capability | list[cap.Capability]:
        if not items and items is not None:
            return []

        actions = (
            [cap.DataModelInstancesAcl.Action.Read]
            if read_only
            else [cap.DataModelInstancesAcl.Action.Read, cap.DataModelInstancesAcl.Action.Write]
        )

        return cap.DataModelInstancesAcl(
            actions=actions, scope=cap.DataModelInstancesAcl.Scope.SpaceID([COGNITE_MIGRATION_SPACE])
        )

    @classmethod
    def get_minimum_scope(cls, items: Sequence[ResourceViewMappingRequest]) -> ScopeDefinition:
        return SpaceIDScope(space_ids=[COGNITE_MIGRATION_SPACE])

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[Acl]:
        if isinstance(scope, AllScope | SpaceIDScope):
            yield DataModelInstancesAcl(actions=sorted(actions), scope=scope)

    def prerequisite_warning(self) -> str | None:
        view_id = RESOURCE_MAPPING_VIEW_ID
        views = self.client.data_modeling.views.retrieve(
            dm.ViewId(space=view_id.space, external_id=view_id.external_id, version=view_id.version)
        )
        if len(views) > 0:
            return None
        return f"{self.display_name} requires the {view_id!s} to be deployed. run `cdf migrate prepare` to deploy it."

    def create(self, items: Sequence[ResourceViewMappingRequest]) -> Sized:
        return self.client.migration.resource_view_mapping.create(items)

    def update(self, items: Sequence[ResourceViewMappingRequest]) -> Sized:
        return self.client.migration.resource_view_mapping.create(items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[ResourceViewMappingResponse]:
        node_ids = NodeId.from_external_ids(ids, space=COGNITE_MIGRATION_SPACE)
        return self.client.migration.resource_view_mapping.retrieve(node_ids)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        node_ids = NodeId.from_external_ids(ids, space=COGNITE_MIGRATION_SPACE)
        result = self.client.migration.resource_view_mapping.delete(node_ids)
        return len(result)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ResourceViewMappingResponse]:
        if space == COGNITE_MIGRATION_SPACE:
            return self.client.migration.resource_view_mapping.list(limit=-1)
        else:
            return []

    @classmethod
    def get_dependent_items(cls, item: dict) -> "Iterable[tuple[type[ResourceCRUD], Hashable]]":
        yield SpaceCRUD, SpaceId(space=COGNITE_MIGRATION_SPACE)
        view_id = RESOURCE_MAPPING_VIEW_ID
        yield (
            ViewCRUD,
            ViewId(space=view_id.space, external_id=view_id.external_id, version=view_id.version),
        )

        if "viewId" in item:
            view_id_dict = item["viewId"]
            if isinstance(view_id_dict, dict) and in_dict(("space", "externalId", "version"), view_id_dict):
                yield (
                    ViewCRUD,
                    ViewId(
                        space=view_id_dict["space"],
                        external_id=view_id_dict["externalId"],
                        version=view_id_dict["version"],
                    ),
                )

    @classmethod
    def get_dependencies(cls, resource: ResourceViewMappingYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        yield SpaceCRUD, SpaceId(space=COGNITE_MIGRATION_SPACE)
        yield ViewCRUD, RESOURCE_MAPPING_VIEW_ID
        if resource.view_id:
            yield ViewCRUD, resource.view_id.as_id()

    def dump_resource(
        self, resource: ResourceViewMappingResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        # Instance type and space is hardcoded for ResourceViewMapping,
        for key in ["existingVersion", "instanceType", "space"]:
            if key not in local:
                dumped.pop(key, None)
        if "viewId" in dumped and "viewId" in local:
            if "type" in dumped["viewId"] and "type" not in local["viewId"]:
                dumped["viewId"].pop("type", None)
        return dumped
