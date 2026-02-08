from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import TypedNodeIdentifier
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    RESOURCE_VIEW_MAPPING_SPACE,
    ResourceViewMappingRequest,
    ResourceViewMappingResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import ResourceViewMappingYAML
from cognite_toolkit._cdf_tk.utils import in_dict, sanitize_filename

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
    def get_id(cls, item: ResourceViewMappingResponse | ResourceViewMappingRequest | dict) -> ExternalId:
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
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        return DataModelInstancesAcl(
            actions=actions, scope=DataModelInstancesAcl.Scope.SpaceID([RESOURCE_VIEW_MAPPING_SPACE])
        )

    def prerequisite_warning(self) -> str | None:
        view_id = ResourceViewMappingRequest.VIEW_ID
        views = self.client.data_modeling.views.retrieve((view_id.space, view_id.external_id, view_id.version))
        if len(views) > 0:
            return None
        return (
            f"{self.display_name} requires the {ResourceViewMappingRequest.VIEW_ID!r} to be deployed. "
            f"run `cdf migrate prepare` to deploy it."
        )

    def create(self, items: Sequence[ResourceViewMappingRequest]) -> list[InstanceSlimDefinition]:
        return self.client.migration.resource_view_mapping.create(items)

    def update(self, items: Sequence[ResourceViewMappingRequest]) -> list[InstanceSlimDefinition]:
        return self.client.migration.resource_view_mapping.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[ResourceViewMappingResponse]:
        return self.client.migration.resource_view_mapping.retrieve(
            TypedNodeIdentifier.from_external_ids(ids, space=RESOURCE_VIEW_MAPPING_SPACE)
        )

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        result = self.client.migration.resource_view_mapping.delete(
            TypedNodeIdentifier.from_external_ids(ids, space=RESOURCE_VIEW_MAPPING_SPACE)
        )
        return len(result)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ResourceViewMappingResponse]:
        if space == RESOURCE_VIEW_MAPPING_SPACE:
            return self.client.migration.resource_view_mapping.list(limit=-1)
        else:
            return []

    @classmethod
    def get_dependent_items(cls, item: dict) -> "Iterable[tuple[type[ResourceCRUD], Hashable]]":
        yield SpaceCRUD, RESOURCE_VIEW_MAPPING_SPACE

        yield (
            ViewCRUD,
            ViewId(
                ResourceViewMappingRequest.VIEW_ID.space,
                ResourceViewMappingRequest.VIEW_ID.external_id,
                ResourceViewMappingRequest.VIEW_ID.version,
            ),
        )

        if "viewId" in item:
            view_id = item["viewId"]
            if isinstance(view_id, dict) and in_dict(("space", "externalId"), view_id):
                yield ViewCRUD, ViewId.load(view_id)

    def dump_resource(
        self, resource: ResourceViewMappingResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        dumped.pop("instanceType", None)
        dumped.pop("space", None)
        return dumped
