# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
import sys
import time
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from time import sleep
from typing import Any, final

from cognite.client.data_classes import filters
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
)
from cognite.client.data_classes.data_modeling import ContainerId, DataModelId, ViewId
from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel

from cognite_toolkit._cdf_tk import constants
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    ContainerFilter,
    DataModelFilter,
    InstanceFilter,
    ViewFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerReference,
    ContainerRequest,
    ContainerResponse,
    DataModelReference,
    DataModelRequest,
    DataModelResponse,
    DirectNodeRelation,
    EdgeRequest,
    EdgeResponse,
    NodeRequest,
    NodeResponse,
    RequiresConstraintDefinition,
    SpaceReference,
    SpaceRequest,
    SpaceResponse,
    ViewCorePropertyResponse,
    ViewReference,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedEdgeIdentifier,
    TypedNodeIdentifier,
    TypedViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.graphql_data_models import (
    GraphQLDataModel,
    GraphQLDataModelList,
    GraphQLDataModelWrite,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING, HAS_DATA_FILTER_LIMIT
from cognite_toolkit._cdf_tk.cruds._base_cruds import (
    ResourceContainerCRUD,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import GraphQLParseError, ToolkitCycleError, ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.resource_classes import (
    ContainerYAML,
    DataModelYAML,
    EdgeYAML,
    GraphQLDataModelYAML,
    NodeYAML,
    SpaceYAML,
    ViewYAML,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import (
    GraphQLParser,
    calculate_hash,
    in_dict,
    load_yaml_inject_variables,
    quote_int_value_by_key_in_yaml,
    safe_read,
    sanitize_filename,
    to_diff,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_identifiable, dm_identifier

from .auth import GroupAllScopedCRUD


@final
class SpaceCRUD(ResourceContainerCRUD[SpaceReference, SpaceRequest, SpaceResponse]):
    item_name = "nodes and edges"
    folder_name = "data_modeling"
    resource_cls = SpaceResponse
    resource_write_cls = SpaceRequest
    kind = "Space"
    yaml_cls = SpaceYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    _doc_url = "Spaces/operation/ApplySpaces"
    delete_recreate_limit_seconds: int = 10

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None) -> None:
        super().__init__(client, build_dir, console)
        self._deleted_time_by_id: dict[SpaceReference, float] = {}

    @property
    def display_name(self) -> str:
        return "spaces"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SpaceRequest] | None, read_only: bool
    ) -> list[Capability] | list[Capability]:
        if not items and items is not None:
            return []

        actions = [DataModelsAcl.Action.Read] if read_only else [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write]

        return [DataModelsAcl(actions, DataModelsAcl.Scope.All())]

    @classmethod
    def get_id(cls, item: SpaceRequest | SpaceResponse | dict) -> SpaceReference:
        if isinstance(item, dict):
            return SpaceReference(space=item["space"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: SpaceReference) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: SpaceReference) -> str:
        return sanitize_filename(id.space)

    def dump_resource(self, resource: SpaceResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        has_local = local is not None
        local = local or {}
        for key in ["description", "name"]:
            if has_local and dumped.get(key) is None and key not in local:
                # Set to null by server.
                dumped.pop(key, None)
        return dumped

    def create(self, items: Sequence[SpaceRequest]) -> list[SpaceResponse]:
        for item in items:
            item_id = self.get_id(item)
            if item_id in self._deleted_time_by_id:
                elapsed_since_delete = time.perf_counter() - self._deleted_time_by_id[item_id]
                if elapsed_since_delete < self.delete_recreate_limit_seconds:
                    time.sleep(self.delete_recreate_limit_seconds - elapsed_since_delete)
        return self.client.tool.spaces.create(items)

    def retrieve(self, ids: SequenceNotStr[SpaceReference]) -> list[SpaceResponse]:
        return self.client.tool.spaces.retrieve(list(ids))

    def update(self, items: Sequence[SpaceRequest]) -> list[SpaceResponse]:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[SpaceReference]) -> int:
        existing = self.client.tool.spaces.retrieve(list(ids))
        is_global = {space.space for space in existing if space.is_global}
        if is_global:
            print(
                f"  [bold yellow]WARNING:[/] Spaces {list(is_global)} are global and cannot be deleted, skipping delete, for these."
            )
        to_delete = [space_ref for space_ref in ids if space_ref.space not in is_global]
        self.client.tool.spaces.delete(to_delete)
        for item_id in to_delete:
            self._deleted_time_by_id[item_id] = time.perf_counter()
        return len(to_delete)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SpaceResponse]:
        if space:
            yield from self.client.tool.spaces.retrieve([SpaceReference(space=space)])
        else:
            for batch in self.client.tool.spaces.iterate():
                yield from batch

    def count(self, ids: SequenceNotStr[SpaceReference]) -> int:
        # Bug in spec of aggregate requiring view_id to be passed in, so we cannot use it.
        # When this bug is fixed, it will be much faster to use aggregate.
        spaces = [space_ref.space for space_ref in ids]

        return sum(len(batch) for batch in self._iterate_over_nodes(spaces)) + sum(
            len(batch) for batch in self._iterate_over_edges(spaces)
        )

    def drop_data(self, ids: SequenceNotStr[SpaceReference]) -> int:
        spaces = [space_ref.space for space_ref in ids]
        if not spaces:
            return 0
        print(f"[bold]Deleting existing data in spaces {ids}...[/]")
        nr_of_deleted = 0
        for edge_ids in self._iterate_over_edges(spaces):
            self.client.tool.instances.delete(edge_ids)
            nr_of_deleted += len(edge_ids)
        for node_ids in self._iterate_over_nodes(spaces):
            self.client.tool.instances.delete(node_ids)
            nr_of_deleted += len(node_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, spaces: list[str]) -> Iterable[list[TypedNodeIdentifier]]:
        if not spaces:
            return
        filter_ = InstanceFilter(instance_type="node", space=spaces)
        for instances in self.client.tool.instances.iterate(filter=filter_):
            yield [inst.as_id() for inst in instances]  # type: ignore[misc]

    def _iterate_over_edges(self, spaces: list[str]) -> Iterable[list[TypedEdgeIdentifier]]:
        if not spaces:
            return
        filter_ = InstanceFilter(instance_type="edge", space=spaces)
        for instances in self.client.tool.instances.iterate(filter=filter_):
            yield [inst.as_id() for inst in instances]  # type: ignore[misc]


class ContainerCRUD(ResourceContainerCRUD[ContainerReference, ContainerRequest, ContainerResponse]):
    item_name = "nodes and edges"
    folder_name = "data_modeling"
    resource_cls = ContainerResponse
    resource_write_cls = ContainerRequest
    kind = "Container"
    dependencies = frozenset({SpaceCRUD})
    yaml_cls = ContainerYAML
    _doc_url = "Containers/operation/ApplyContainers"
    sub_folder_name = "containers"

    def __init__(
        self,
        client: ToolkitClient,
        build_dir: Path | None,
        console: Console | None = None,
        topological_sort_implements: bool = False,
    ) -> None:
        super().__init__(client, build_dir, console)
        self._container_by_id: dict[ContainerReference, ContainerResponse] = {}

    @property
    def display_name(self) -> str:
        return "containers"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ContainerRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [DataModelsAcl.Action.Read] if read_only else [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write]

        scope = (
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelsAcl.Scope.All()
        )

        return DataModelsAcl(actions, scope)

    @classmethod
    def get_id(cls, item: ContainerRequest | ContainerResponse | dict) -> ContainerReference:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return ContainerReference(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ContainerReference) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])
        # Note that we are very careful in the code below to not raise an exception if the
        # item is not properly formed. If that is the case, an appropriate warning will be given elsewhere.
        for prop in item.get("properties", {}).values():
            if not isinstance(prop, dict):
                continue
            prop_type = prop.get("type", {})
            if isinstance(prop_type, dict) and prop_type.get("type") == "direct":
                if isinstance(prop_type.get("container"), dict):
                    container = prop_type["container"]
                    if "space" in container and "externalId" in container and container.get("type") == "container":
                        yield (
                            ContainerCRUD,
                            ContainerReference(space=container["space"], external_id=container["externalId"]),
                        )

    def dump_resource(self, resource: ContainerResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        has_local = local is not None
        local = local or {}
        for key in ["description", "name"]:
            if has_local and dumped.get(key) is None and key not in local:
                # Set to null by server.
                dumped.pop(key, None)

        for key in ["constraints", "indexes"]:
            if not dumped.get(key) and key not in local:
                # Set to empty dict by server.
                dumped.pop(key, None)
                continue
            if isinstance((cdf_value := dumped.get(key)), dict) and isinstance((local_value := local.get(key)), dict):
                for cdf_id, cdf_item in cdf_value.items():
                    local_item = local_value.get(cdf_id)
                    if (
                        isinstance(local_item, dict)
                        and "bySpace" not in local_item
                        and isinstance(cdf_item, dict)
                        and cdf_item.get("bySpace") is False
                    ):
                        cdf_item.pop("bySpace", None)
        local_prop_by_id = local.get("properties", {})
        for prop_id, cdf_prop in dumped.get("properties", {}).items():
            if prop_id not in local_prop_by_id:
                continue
            local_prop = local_prop_by_id[prop_id]
            for key, default in [("immutable", False), ("autoIncrement", False), ("nullable", True)]:
                if has_local and cdf_prop.get(key) is default and key not in local_prop:
                    cdf_prop.pop(key, None)
            cdf_type = cdf_prop.get("type", {})
            local_type = local_prop.get("type", {})
            for key, type_default in [("list", False), ("collation", "ucs_basic")]:
                if has_local and cdf_type.get(key) == type_default and key not in local_type:
                    cdf_type.pop(key, None)
            if has_local and "usedFor" not in local and dumped.get("usedFor") == "node":
                # Only drop if set to default by server.
                dumped.pop("usedFor", None)
        return dumped

    def create(self, items: Sequence[ContainerRequest]) -> list[ContainerResponse]:
        return self.client.tool.containers.create(items)

    def retrieve(self, ids: SequenceNotStr[ContainerReference]) -> list[ContainerResponse]:
        return self.client.tool.containers.retrieve(list(ids))

    def update(self, items: Sequence[ContainerRequest]) -> list[ContainerResponse]:
        updated = self.create(items)
        # The API might silently fail to update a container.
        updated_by_id = {item.as_id(): item for item in updated}
        for local in items:
            item_id = local.as_id()
            local_dict = local.dump()
            if item_id not in updated_by_id:
                raise ToolkitAPIError(
                    f"The container {item_id} was not updated. You might need to delete and recreate it.",
                    code=500,
                )
            cdf_dict = self.dump_resource(updated_by_id[item_id], local_dict)
            if cdf_dict != local_dict:
                is_verbose = "-v" in sys.argv or "--verbose" in sys.argv
                if is_verbose:
                    print(
                        Panel(
                            "\n".join(to_diff(cdf_dict, local_dict)),
                            title=f"{self.display_name}: {item_id}",
                            expand=False,
                        )
                    )
                suffix = "" if is_verbose else " (use -v for more info)"
                HighSeverityWarning(
                    f"The container {item_id} was not updated. You might need to delete and recreate it{suffix}."
                ).print_warning()
        return updated

    def delete(self, ids: SequenceNotStr[ContainerReference]) -> int:
        self.client.tool.containers.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ContainerResponse]:
        for batch in self.client.tool.containers.iterate(filter=ContainerFilter(space=space) if space else None):
            yield from batch

    def count(self, ids: SequenceNotStr[ContainerReference]) -> int:
        # Bug in spec of aggregate requiring view_id to be passed in, so we cannot use it.
        # When this bug is fixed, it will be much faster to use aggregate.
        existing_containers = self.client.tool.containers.retrieve(list(ids))
        return sum(len(batch) for batch in self._iterate_over_nodes(existing_containers)) + sum(
            len(batch) for batch in self._iterate_over_edges(existing_containers)
        )

    def drop_data(self, ids: SequenceNotStr[ContainerReference]) -> int:
        nr_of_deleted = 0
        existing_containers = self.client.tool.containers.retrieve(list(ids))
        for node_ids in self._iterate_over_nodes(existing_containers):
            self.client.tool.instances.delete(node_ids)
            nr_of_deleted += len(node_ids)
        for edge_ids in self._iterate_over_edges(existing_containers):
            self.client.tool.instances.delete(edge_ids)
            nr_of_deleted += len(edge_ids)
        return nr_of_deleted

    def _iterate_over_nodes(self, containers: list[ContainerResponse]) -> Iterable[list[TypedNodeIdentifier]]:
        container_ids = [container.as_id() for container in containers if container.used_for in ["node", "all"]]
        if not container_ids:
            return
        for container_id_chunk in self._chunker(container_ids, HAS_DATA_FILTER_LIMIT):
            is_container = filters.HasData(
                containers=[ContainerId(space=cid.space, external_id=cid.external_id) for cid in container_id_chunk]
            )
            for instances in self.client.data_modeling.instances(
                chunk_size=1000, instance_type="node", filter=is_container, limit=-1
            ):
                yield [TypedNodeIdentifier(space=nid.space, external_id=nid.external_id) for nid in instances.as_ids()]

    def _iterate_over_edges(self, containers: list[ContainerResponse]) -> Iterable[list[TypedEdgeIdentifier]]:
        container_ids = [container.as_id() for container in containers if container.used_for in ["edge", "all"]]
        if not container_ids:
            return

        for container_id_chunk in self._chunker(container_ids, HAS_DATA_FILTER_LIMIT):
            is_container = filters.HasData(
                containers=[ContainerId(space=cid.space, external_id=cid.external_id) for cid in container_id_chunk]
            )
            for instances in self.client.data_modeling.instances(
                chunk_size=1000, instance_type="edge", limit=-1, filter=is_container
            ):
                yield [TypedEdgeIdentifier(space=eid.space, external_id=eid.external_id) for eid in instances.as_ids()]

    def _lookup_containers(
        self, container_ids: Sequence[ContainerReference]
    ) -> dict[ContainerReference, ContainerResponse]:
        ids_to_lookup = [container_id for container_id in container_ids if container_id not in self._container_by_id]
        if ids_to_lookup:
            retrieved_containers = self.client.tool.containers.retrieve(ids_to_lookup)
            for container in retrieved_containers:
                self._container_by_id[container.as_id()] = container
        if missing_container_ids := set(container_ids) - set(self._container_by_id.keys()):
            MediumSeverityWarning(
                f"Containers {missing_container_ids} not found or you don't have permission to access them."
            ).print_warning(console=self.console)
        return {
            container_id: self._container_by_id[container_id]
            for container_id in container_ids
            if container_id in self._container_by_id
        }

    def _find_direct_container_dependencies(
        self, container_ids: Sequence[ContainerReference]
    ) -> dict[ContainerReference, set[ContainerReference]]:
        containers_by_id = self._lookup_containers(container_ids)
        container_dependencies: dict[ContainerReference, set[ContainerReference]] = defaultdict(set)
        for container_id, container in containers_by_id.items():
            for constraint in (container.constraints or {}).values():
                if not isinstance(constraint, RequiresConstraintDefinition):
                    continue
                container_dependencies[container_id].add(constraint.require)
            for property in container.properties.values():
                if not isinstance(property.type, DirectNodeRelation) or property.type.container is None:
                    continue
                container_dependencies[container_id].add(property.type.container)
        return container_dependencies

    def _propagate_indirect_container_dependencies(
        self,
        container_dependencies_by_id: dict[ContainerReference, set[ContainerReference]],
        dependants: Sequence[ContainerReference],
    ) -> dict[ContainerReference, set[ContainerReference]]:
        """Propagate indirect container dependencies using a recursive approach.

        Args:
            container_dependencies_by_id: Mapping of container IDs to their direct dependencies
            dependants: Chain of dependant containers to propagate dependencies to

        Returns:
            Updated dictionary mapping each container ID to all its direct and indirect dependencies
        """
        current_container_id = dependants[0]
        dependencies_to_propagate: set[ContainerReference] = set()
        for container_dependency in container_dependencies_by_id[current_container_id]:
            if container_dependency in container_dependencies_by_id:
                # If already processed, propagate its dependencies to current container instead of revisiting it
                dependencies_to_propagate.update(container_dependencies_by_id[container_dependency])
                continue
            self._propagate_indirect_container_dependencies(
                container_dependencies_by_id, [container_dependency, *dependants]
            )
        container_dependencies_by_id[current_container_id].update(dependencies_to_propagate)
        return container_dependencies_by_id

    def _find_direct_and_indirect_container_dependencies(
        self, container_ids: Sequence[ContainerReference]
    ) -> dict[ContainerReference, set[ContainerReference]]:
        container_dependencies_by_id = self._find_direct_container_dependencies(container_ids)
        for container_id in list(container_dependencies_by_id.keys()):
            self._propagate_indirect_container_dependencies(container_dependencies_by_id, [container_id])
        return container_dependencies_by_id

    @staticmethod
    def _chunker(seq: Sequence, size: int) -> Iterable[Sequence]:
        return (seq[pos : pos + size] for pos in range(0, len(seq), size))

    @classmethod
    def as_str(cls, id: ContainerReference) -> str:
        return sanitize_filename(f"{id.space}_{id.external_id}")


class ViewCRUD(ResourceCRUD[ViewReference, ViewRequest, ViewResponse]):
    folder_name = "data_modeling"
    resource_cls = ViewResponse
    resource_write_cls = ViewRequest
    kind = "View"
    dependencies = frozenset({SpaceCRUD, ContainerCRUD})
    yaml_cls = ViewYAML
    _doc_url = "Views/operation/ApplyViews"
    sub_folder_name = "views"

    def __init__(
        self,
        client: ToolkitClient,
        build_dir: Path | None,
        console: Console | None,
        topological_sort_implements: bool = False,
    ) -> None:
        super().__init__(client, build_dir, console)
        self._topological_sort_implements = topological_sort_implements
        self._view_by_id: dict[ViewReference, ViewResponse] = {}

    @property
    def display_name(self) -> str:
        return "views"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ViewRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [DataModelsAcl.Action.Read] if read_only else [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write]

        scope = (
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelsAcl.Scope.All()
        )

        return DataModelsAcl(actions, scope)

    @classmethod
    def get_id(cls, item: ViewRequest | ViewResponse | dict) -> ViewReference:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return ViewReference(space=item["space"], external_id=item["externalId"], version=str(item["version"]))

        return item.as_id()

    @classmethod
    def dump_id(cls, id: ViewReference) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])
        if isinstance(implements := item.get("implements", []), list):
            for parent in implements:
                if not isinstance(parent, dict):
                    continue
                if parent.get("type") == "view" and in_dict(["space", "externalId", "version"], parent):
                    yield (
                        ViewCRUD,
                        ViewReference(
                            space=parent["space"],
                            external_id=parent["externalId"],
                            version=str(v) if (v := parent.get("version")) else "",
                        ),
                    )
        for prop in item.get("properties", {}).values():
            if (container := prop.get("container", {})) and container.get("type") == "container":
                if in_dict(("space", "externalId"), container):
                    yield (
                        ContainerCRUD,
                        ContainerReference(space=container["space"], external_id=container["externalId"]),
                    )
            for key, dct_ in [("source", prop), ("edgeSource", prop), ("source", prop.get("through", {}))]:
                if source := dct_.get(key, {}):
                    if source.get("type") == "view" and in_dict(("space", "externalId", "version"), source):
                        yield (
                            ViewCRUD,
                            ViewReference(
                                space=source["space"],
                                external_id=source["externalId"],
                                version=str(v) if (v := source.get("version")) else "",
                            ),
                        )
                    elif source.get("type") == "container" and in_dict(("space", "externalId"), source):
                        yield ContainerCRUD, ContainerReference(space=source["space"], external_id=source["externalId"])

    def safe_read(self, filepath: Path | str) -> str:
        # The version is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="version")

    def dump_resource(self, resource: ViewResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if not dumped.get("properties") and not local.get("properties"):
            if "properties" in local:
                # In case the properties is an empty dict, we still want to keep it in the dump.
                # such that the dumped evaluates to the same as the local.
                dumped["properties"] = local["properties"]
            else:
                dumped.pop("properties", None)
        if not dumped.get("implements") and not local.get("implements"):
            if "implements" in local:
                # In case the implements is an empty list, we still want to keep it in the dump.
                # such that the dumped evaluates to the same as the local.
                dumped["implements"] = local["implements"]
            else:
                dumped.pop("implements", None)
        if resource.implements and len(resource.implements) > 1 and self._topological_sort_implements:
            # This is a special case that we want to do when we run the cdf dump datamodel command.
            # The issue is as follows:
            # 1. If a data model is deployed through GraphQL, the implements for a child view are sorted
            #   from parent, grandparent, etc.
            # 2. If the grand parent has a direct relation that the parent overwrites to update the source.
            #   The child will get the grandparent's source, not the parent's.
            # We sort the implements in topological order to ensure that the child view get the order grandparent,
            # parent, such that the parent's source is used.
            try:
                dumped["implements"] = [
                    view_id.dump() for view_id in self.topological_sort_implements(resource.implements)
                ]
            except ToolkitCycleError as e:
                warning = MediumSeverityWarning(f"Failed to sort implements for view {resource.as_id()}: {e}")
                warning.print_warning(console=self.console)

        local_properties = local.get("properties", {})
        for prop_id, prop in dumped.get("properties", {}).items():
            if prop_id not in local_properties:
                continue
            local_prop = local_properties[prop_id]
            if all(isinstance(v.get("container"), dict) for v in [prop, local_prop]):
                if prop["container"].get("type") == "container" and "type" not in local_prop["container"]:
                    prop["container"].pop("type", None)
            is_connection_prop = "connectionType" in prop
            is_local_connection_prop = "connectionType" in local_prop
            if (
                is_connection_prop
                and is_local_connection_prop
                and "direction" not in local_prop
                and prop.get("direction") == "outwards"
            ):
                # The API will set the direction to outwards by default, so we remove it from the dump.
                prop.pop("direction", None)
            for key, default in [("description", None), ("name", None), ("source", None), ("edgeSource", None)]:
                if prop.get(key) == default and key not in local_prop:
                    prop.pop(key, None)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("implements",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        try:
            return self.client.tool.views.create(items)
        except ToolkitAPIError as e1:
            if e1.is_auto_retryable:
                # Fallback to creating one by one if the error is auto-retryable.
                return self._fallback_create_one_by_one(items, e1)
            raise

    def _fallback_create_one_by_one(
        self, items: Sequence[ViewRequest], e1: ToolkitAPIError, warn: bool = True
    ) -> list[ViewResponse]:
        if warn:
            MediumSeverityWarning(
                f"Failed to create {len(items)} views error:\n{escape(str(e1))}\n\n----------------------------\nTrying to create one by one..."
            ).print_warning(include_timestamp=True, console=self.console)
        created_list: list[ViewResponse] = []
        for no, item in enumerate(items):
            try:
                created = self.client.tool.views.create([item])
            except ToolkitAPIError as e2:
                raise e2 from e1
            else:
                created_list.extend(created)
        return created_list

    def retrieve(self, ids: SequenceNotStr[ViewReference]) -> list[ViewResponse]:
        return self.client.tool.views.retrieve(list(ids), include_inherited_properties=False)

    def update(self, items: Sequence[ViewRequest]) -> list[ViewResponse]:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[ViewReference]) -> int:
        to_delete = list(ids)
        nr_of_deleted = 0
        attempt_count = 5
        for attempt_no in range(attempt_count):
            self.client.tool.views.delete(to_delete)
            nr_of_deleted += len(to_delete)
            existing = [view.as_id() for view in self.client.tool.views.retrieve(to_delete)]
            if not existing:
                return nr_of_deleted
            sleep(2)
            to_delete = existing
        else:
            msg = f"  [bold yellow]WARNING:[/] Could not delete views {to_delete} after {attempt_count} attempts."
            if self.console:
                self.console.print(msg)
            else:
                print(msg)
        return nr_of_deleted

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ViewResponse]:
        for batch in self.client.tool.views.iterate(filter=ViewFilter(space=space) if space else None):
            yield from batch

    @classmethod
    def as_str(cls, id: ViewReference) -> str:
        return sanitize_filename(id.external_id)

    def _lookup_views(self, view_ids: list[ViewReference]) -> dict[ViewReference, ViewResponse]:
        """Looks up views by their IDs and caches them."""
        missing_ids = [view_id for view_id in view_ids if view_id not in self._view_by_id]
        if missing_ids:
            retrieved_views = self.client.tool.views.retrieve(missing_ids, include_inherited_properties=False)
            for view in retrieved_views:
                self._view_by_id[view.as_id()] = view
        return {view_id: self._view_by_id[view_id] for view_id in view_ids if view_id in self._view_by_id}

    def get_readonly_properties(self, view_id: ViewReference) -> set[str]:
        """Retrieve the set of read-only properties for a given view."""

        readonly_properties: set[str] = set()

        # Retrieve the view to check its properties
        view = self._lookup_views([view_id]).get(view_id)
        if view is None:
            return readonly_properties

        # Check each property in the view
        for property_identifier, property in view.properties.items():
            if isinstance(
                property, ViewCorePropertyResponse
            ) and property.container_property_identifier in constants.READONLY_CONTAINER_PROPERTIES.get(
                property.container.as_tuple(), set()
            ):
                readonly_properties.add(property_identifier)
        return readonly_properties

    def _build_view_implements_dependencies(
        self, view_by_ids: dict[ViewReference, ViewResponse], include: set[ViewReference] | None = None
    ) -> dict[ViewReference, set[ViewReference]]:
        """Build a dependency graph based on view implements relationships.

        Args:
            view_by_ids: Mapping of view IDs to ViewResponse objects
            include: Optional set of view IDs to include in the dependencies, if None, all views are included.

        Returns:
            Dictionary mapping each view ID to the set of view IDs it depends on (implements)
        """
        dependencies: dict[ViewReference, set[ViewReference]] = {}
        for view_id, view in view_by_ids.items():
            dependencies[view_id] = set()
            for implemented_view_id in view.implements or []:
                if include is None or implemented_view_id in include:
                    dependencies[view_id].add(implemented_view_id)
        return dependencies

    def topological_sort_implements(self, view_ids: list[ViewReference]) -> list[ViewReference]:
        """Sorts the views in topological order based on their implements and through properties."""
        view_by_ids = self._lookup_views(view_ids)
        parents_by_child = self._build_view_implements_dependencies(view_by_ids)

        try:
            sorted_views = list(TopologicalSorter(parents_by_child).static_order())
        except CycleError as e:
            raise ToolkitCycleError(
                f"Failed to sort views topologically. This likely due to a cycle in implements. {e.args[1]}"
            )

        return sorted_views

    def topological_sort_container_constraints(
        self, view_ids: list[ViewReference]
    ) -> tuple[list[ViewReference], list[ViewReference]]:
        """Sorts the views in topological order based on their container constraints.

        Returns:
            A tuple containing the sorted views and cyclic views that could not be sorted (if any).
        """

        view_by_ids = self._lookup_views(view_ids)
        if missing_view_ids := set(view_ids) - set(view_by_ids.keys()):
            MediumSeverityWarning(
                f"Views {missing_view_ids} not found or you don't have permission to access them, skipping dependency check."
            ).print_warning(console=self.console)
            return view_ids, []

        view_to_containers: dict[ViewReference, set[ContainerReference]] = {}
        container_to_views: defaultdict[ContainerReference, set[ViewReference]] = defaultdict(set)
        for view_id, view in view_by_ids.items():
            view_to_containers[view_id] = set(view.mapped_containers)
            for container_id in view_to_containers[view_id]:
                container_to_views[container_id].add(view_id)

        container_crud = ContainerCRUD.create_loader(self.client)
        container_dependencies_by_id = container_crud._find_direct_and_indirect_container_dependencies(
            list(container_to_views.keys())
        )

        # First, add view dependencies based on implements relationships
        view_dependencies = self._build_view_implements_dependencies(view_by_ids, set(view_to_containers.keys()))

        # Then, add view dependencies based on mapped container constraints
        for view_id, mapped_containers in view_to_containers.items():
            for container_id in mapped_containers:
                # Get all containers this container depends on
                if container_id not in container_dependencies_by_id:
                    continue
                for required_container in container_dependencies_by_id[container_id]:
                    if required_container not in container_to_views:
                        continue
                    # If this view already implements the required container, the requirement is self-satisfied
                    # and we don't need to depend on other views that also implement it (they are peers).
                    if required_container in mapped_containers:
                        continue
                    # This view doesn't implement the required container, so depend on all views that do
                    view_dependencies[view_id].update(container_to_views[required_container])

        cyclic_views: list[ViewReference] = []
        for _ in range(
            len(view_dependencies)
        ):  # Ensure an upper bound on the number of iterations we do when removing cycles.
            try:
                TopologicalSorter(view_dependencies).prepare()
                break
            except CycleError as e:
                cycle_nodes = set(e.args[1])
                cyclic_views.extend(cycle_nodes)
                view_dependencies = {k: v - cycle_nodes for k, v in view_dependencies.items() if k not in cycle_nodes}

        sorted_views = list(TopologicalSorter(view_dependencies).static_order())

        return sorted_views, cyclic_views


@final
class DataModelCRUD(ResourceCRUD[DataModelReference, DataModelRequest, DataModelResponse]):
    folder_name = "data_modeling"
    resource_cls = DataModelResponse
    resource_write_cls = DataModelRequest
    kind = "DataModel"
    dependencies = frozenset({SpaceCRUD, ViewCRUD})
    yaml_cls = DataModelYAML
    _doc_url = "Data-models/operation/createDataModels"

    @property
    def display_name(self) -> str:
        return "data models"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataModelRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [DataModelsAcl.Action.Read] if read_only else [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write]

        scope = (
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelsAcl.Scope.All()
        )

        return DataModelsAcl(actions, scope)

    @classmethod
    def get_id(cls, item: DataModelRequest | DataModelResponse | dict) -> DataModelReference:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return DataModelReference(space=item["space"], external_id=item["externalId"], version=str(item["version"]))
        return item.as_id()

    @classmethod
    def dump_id(cls, id: DataModelReference) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])
        for view in item.get("views", []):
            if in_dict(("space", "externalId"), view):
                yield (
                    ViewCRUD,
                    ViewReference(
                        space=view["space"],
                        external_id=view["externalId"],
                        version=str(v) if (v := view.get("version")) else "",
                    ),
                )

    def safe_read(self, filepath: Path | str) -> str:
        # The version is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="version")

    def dump_resource(self, resource: DataModelResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if "views" not in dumped:
            return dumped
        for key in ["name", "description"]:
            if dumped.get(key) is None and key not in local:
                # Set to null by server.
                dumped.pop(key, None)
        # Sorting in the same order as the local file.
        view_order_by_id = {ViewReference._load(v): no for no, v in enumerate(local.get("views", []))}
        end_of_list = len(view_order_by_id)
        dumped["views"] = sorted(
            dumped["views"], key=lambda v: view_order_by_id.get(ViewReference._load(v), end_of_list)
        )
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("views",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        return self.client.tool.data_models.create(items)

    def retrieve(self, ids: SequenceNotStr[DataModelReference]) -> list[DataModelResponse]:
        return self.client.tool.data_models.retrieve(list(ids))

    def update(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        updated = self.create(items)
        # There is a bug in the API not raising an exception if view is removed from a data model.
        # So we check here that the update was fixed.
        updated_by_id = {item.as_id(): item for item in updated}
        for local in items:
            item_id = local.as_id()
            if item_id in updated_by_id:
                views_updated = set(updated_by_id[item_id].views or [])
                views_local = set(local.views or [])
                missing = views_local - views_updated
                extra = views_updated - views_local
                if missing or extra:
                    raise ToolkitAPIError(
                        f"The API did not update the data model, {item_id} correctly. You might have "
                        f"to increase the version number of the data model for it to update.\nMissing views in CDF: {missing}\n"
                        f"Extra views in the CDF: {extra}",
                        code=500,
                    )
            else:
                raise ToolkitAPIError(
                    f"The data model {item_id} was not updated. Please check the data model manually.",
                    code=500,
                )

        return updated

    def delete(self, ids: SequenceNotStr[DataModelReference]) -> int:
        self.client.tool.data_models.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataModelResponse]:
        for batch in self.client.tool.data_models.iterate(filter=DataModelFilter(space=space, include_global=False)):
            yield from batch

    @classmethod
    def as_str(cls, id: DataModelReference) -> str:
        return sanitize_filename(id.external_id)


@final
class NodeCRUD(ResourceContainerCRUD[TypedNodeIdentifier, NodeRequest, NodeResponse]):
    item_name = "nodes"
    folder_name = "data_modeling"
    resource_cls = NodeResponse
    resource_write_cls = NodeRequest
    kind = "Node"
    yaml_cls = NodeYAML
    dependencies = frozenset({SpaceCRUD, ViewCRUD, ContainerCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"
    sub_folder_name = "nodes"

    def __init__(
        self,
        client: ToolkitClient,
        build_dir: Path | None,
        console: Console | None = None,
        view_id: TypedViewReference | None = None,
    ) -> None:
        super().__init__(client, build_dir, console)
        # View ID is used to retrieve nodes with properties.
        self.view_id = view_id

    @property
    def display_name(self) -> str:
        return "nodes"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[NodeRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        return DataModelInstancesAcl(
            actions,
            DataModelInstancesAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelInstancesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: NodeRequest | NodeResponse | dict) -> TypedNodeIdentifier:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return TypedNodeIdentifier(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: TypedNodeIdentifier) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])
        for source in item.get("sources", []):
            if (identifier := source.get("source")) and isinstance(identifier, dict):
                if identifier.get("type") == "view" and in_dict(("space", "externalId", "version"), identifier):
                    yield (
                        ViewCRUD,
                        ViewReference(
                            space=identifier["space"],
                            external_id=identifier["externalId"],
                            version=str(v) if (v := identifier.get("version")) else "",
                        ),
                    )
                elif identifier.get("type") == "container" and in_dict(("space", "externalId"), identifier):
                    yield (
                        ContainerCRUD,
                        ContainerReference(space=identifier["space"], external_id=identifier["externalId"]),
                    )

    def dump_resource(self, resource: NodeResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        # CDF resource does not have properties set, so we need to do a lookup
        local = local or {}
        sources = [
            TypedViewReference._load(source["source"]) for source in local.get("sources", []) if "source" in source
        ]

        # Default dump
        dumped = resource.as_request_resource().dump()
        if sources:
            try:
                node_id = resource.as_id()
                res = self.client.tool.instances.retrieve([node_id], source=sources[0])
            except ToolkitAPIError:
                ...
            else:
                if res:
                    # Dump with properties populated.
                    dumped = res[0].as_request_resource().dump()

        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)

        if "instanceType" in dumped and "instanceType" not in local:
            # Toolkit uses file suffix to determine instanceType, so we need to remove it from the CDF resource
            # to match the local resource.
            dumped.pop("instanceType")

        return dumped

    def create(self, items: Sequence[NodeRequest]) -> list[InstanceSlimDefinition]:
        return self.client.tool.instances.create(list(items))

    def retrieve(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> list[NodeResponse]:
        source_ref = (
            TypedViewReference(
                space=self.view_id.space, external_id=self.view_id.external_id, version=self.view_id.version
            )
            if self.view_id
            else None
        )
        results = self.client.tool.instances.retrieve(list(ids), source=source_ref)
        return [r for r in results if isinstance(r, NodeResponse)]

    def update(self, items: Sequence[NodeRequest]) -> list[InstanceSlimDefinition]:
        return self.client.tool.instances.create(list(items))

    def delete(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> int:
        try:
            deleted = self.client.tool.instances.delete(list(ids))
        except ToolkitAPIError as e:
            if "not exist" in str(e) and "space" in str(e).lower():
                return 0
            raise e
        return len(deleted)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[NodeResponse]:
        source_ref = (
            TypedViewReference(
                space=self.view_id.space, external_id=self.view_id.external_id, version=self.view_id.version
            )
            if self.view_id
            else None
        )
        filter_ = InstanceFilter(
            instance_type="node",
            space=[space] if space else None,
            source=source_ref,
        )
        for batch in self.client.tool.instances.iterate(filter=filter_):
            for inst in batch:
                if isinstance(inst, NodeResponse):
                    yield inst

    def count(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> int:
        return len(ids)

    def drop_data(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> int:
        # Nodes will be deleted in .delete call.
        return 0

    @classmethod
    def as_str(cls, id: TypedNodeIdentifier) -> str:
        return sanitize_filename(f"{id.space}_{id.external_id}")


class GraphQLCRUD(ResourceContainerCRUD[DataModelId, GraphQLDataModelWrite, GraphQLDataModel]):
    folder_name = "data_modeling"
    resource_cls = GraphQLDataModel
    resource_write_cls = GraphQLDataModelWrite
    kind = "GraphQLSchema"
    dependencies = frozenset({SpaceCRUD, ContainerCRUD})
    item_name = "views"
    yaml_cls = GraphQLDataModelYAML
    _doc_url = "Data-models/operation/createDataModels"
    _hash_name = "CDFToolkitHash:"

    def __init__(self, client: ToolkitClient, build_dir: Path, console: Console | None) -> None:
        super().__init__(client, build_dir, console)
        self._graphql_filepath_cache: dict[DataModelId, Path] = {}
        self._datamodels_by_view_id: dict[ViewId, set[DataModelId]] = defaultdict(set)
        self._dependencies_by_datamodel_id: dict[DataModelId, set[ViewId | DataModelId]] = {}

    @property
    def display_name(self) -> str:
        return "graph QL schemas"

    @classmethod
    def get_id(cls, item: GraphQLDataModelWrite | GraphQLDataModel | dict) -> DataModelId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return DataModelId(space=item["space"], external_id=item["externalId"], version=str(item["version"]))
        return DataModelId(item.space, item.external_id, str(item.version))

    @classmethod
    def dump_id(cls, id: DataModelId) -> dict[str, Any]:
        return id.dump(include_type=False)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[GraphQLDataModelWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        actions = [DataModelsAcl.Action.Read] if read_only else [DataModelsAcl.Action.Read, DataModelsAcl.Action.Write]
        return DataModelsAcl(
            actions,
            DataModelsAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelsAcl.Scope.All(),
        )

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])

    def safe_read(self, filepath: Path | str) -> str:
        # The version is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="version")

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_yaml = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )
        raw_list = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]

        for item in raw_list:
            model_id = self.get_id(item)
            # Find the GraphQL files adjacent to the DML files
            graphql_file = filepath.with_suffix(".graphql")
            if not graphql_file.is_file():
                raise ToolkitFileNotFoundError(
                    f"Failed to find GraphQL file. Expected {graphql_file.name} adjacent to {filepath.as_posix()}"
                )

            self._graphql_filepath_cache[model_id] = graphql_file
            graphql_content = safe_read(graphql_file, encoding=BUILD_FOLDER_ENCODING)

            parser = GraphQLParser(graphql_content, model_id)
            try:
                for view in parser.get_views():
                    self._datamodels_by_view_id[view].add(model_id)
                self._dependencies_by_datamodel_id[model_id] = parser.get_dependencies()
            except Exception as e:
                # We catch a broad exception here to give a more user-friendly error message.
                raise GraphQLParseError(f"Failed to parse GraphQL file {graphql_file.as_posix()}: {e}") from e

            # Add hash to description
            description = item.get("description", "")
            hash_ = calculate_hash(graphql_content)[:8]
            suffix = f"{self._hash_name}{hash_}"
            if len(description) + len(suffix) > 1024:
                LowSeverityWarning(f"Description is above limit for {model_id}. Truncating...").print_warning()
                description = description[: 1024 - len(suffix) + 1 - 3] + "..."
            description += f" {suffix}"
            item["description"] = description
            item["graphqlFile"] = hash_
        return raw_list

    def dump_resource(self, resource: GraphQLDataModel, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        for key in ["dml", "preserveDml"]:
            # Local values that are not returned from the API
            if key in local:
                dumped[key] = local[key]

        description = resource.description or ""
        if match := re.match(rf"(.|\n)*( {self._hash_name}([a-f0-9]{{8}}))$", description):
            dumped["graphqlFile"] = match.group(3)
        return dumped

    def create(self, items: Sequence[GraphQLDataModelWrite]) -> list[DMLApplyResult]:
        creation_order = self._topological_sort(items)

        created_list: list[DMLApplyResult] = []
        for item in creation_order:
            item_id = item.as_id()
            graphql_file_content = self._get_graphql_content(item_id)
            if "--verbose" in sys.argv:
                print(f"Deploying GraphQL schema {item_id}")

            created = self.client.dml.apply_dml(
                item.as_id(),
                dml=graphql_file_content,
                name=item.name,
                description=item.description,
                previous_version=item.previous_version,
                preserve_dml=item.preserve_dml,
            )
            created_list.append(created)
        return created_list

    def _get_graphql_content(self, data_model_id: DataModelId) -> str:
        filepath = self._graphql_filepath_cache.get(data_model_id)
        if filepath is None:
            raise ToolkitFileNotFoundError(f"Could not find the GraphQL file for {data_model_id}")
        return safe_read(filepath)

    def retrieve(self, ids: SequenceNotStr[DataModelId]) -> GraphQLDataModelList:
        result = self.client.data_modeling.data_models.retrieve(list(ids), inline_views=False)
        return GraphQLDataModelList([GraphQLDataModel._load(d.dump()) for d in result])

    def update(self, items: Sequence[GraphQLDataModelWrite]) -> list[DMLApplyResult]:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[DataModelId]) -> int:
        retrieved = self.retrieve(ids)
        views = {view for dml in retrieved for view in dml.views or []}
        deleted = len(self.client.data_modeling.data_models.delete(list(ids)))
        deleted += len(self.client.data_modeling.views.delete(list(views)))
        return deleted

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[GraphQLDataModel]:
        return iter(GraphQLDataModel._load(d.dump()) for d in self.client.data_modeling.data_models)

    def count(self, ids: SequenceNotStr[DataModelId]) -> int:
        retrieved = self.retrieve(ids)
        return sum(len(d.views or []) for d in retrieved)

    def drop_data(self, ids: SequenceNotStr[DataModelId]) -> int:
        return self.delete(ids)

    def _topological_sort(self, items: Sequence[GraphQLDataModelWrite]) -> list[GraphQLDataModelWrite]:
        to_sort = {item.as_id(): item for item in items}
        dependencies: dict[DataModelId, set[DataModelId]] = {}
        for item in items:
            item_id = item.as_id()
            dependencies[item_id] = set()
            for dependency in self._dependencies_by_datamodel_id.get(item_id, []):
                if isinstance(dependency, DataModelId) and dependency in to_sort:
                    dependencies[item_id].add(dependency)
                elif isinstance(dependency, ViewId):
                    for model_id in self._datamodels_by_view_id.get(dependency, set()):
                        if model_id in to_sort:
                            dependencies[item_id].add(model_id)
        try:
            return [to_sort[item_id] for item_id in TopologicalSorter(dependencies).static_order()]
        except CycleError as e:
            raise ToolkitCycleError(
                f"Cannot create GraphQL schemas. Cycle detected between models {e.args} using the @import directive.",
                *e.args[1:],
            )


@final
class EdgeCRUD(ResourceContainerCRUD[TypedEdgeIdentifier, EdgeRequest, EdgeResponse]):
    item_name = "edges"
    folder_name = "data_modeling"
    resource_cls = EdgeResponse
    resource_write_cls = EdgeRequest
    kind = "Edge"
    yaml_cls = EdgeYAML
    dependencies = frozenset({SpaceCRUD, ViewCRUD, ContainerCRUD, NodeCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "edges"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[EdgeRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        return DataModelInstancesAcl(
            actions,
            DataModelInstancesAcl.Scope.SpaceID(list({item.space for item in items}))
            if items is not None
            else DataModelInstancesAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: EdgeRequest | EdgeResponse | dict) -> TypedEdgeIdentifier:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"space", "externalId"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return TypedEdgeIdentifier(space=item["space"], external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: TypedEdgeIdentifier) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: TypedEdgeIdentifier) -> str:
        return sanitize_filename(f"{id.space}_{id.external_id}")

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "space" in item:
            yield SpaceCRUD, SpaceReference(space=item["space"])
        for source in item.get("sources", []):
            if (identifier := source.get("source")) and isinstance(identifier, dict):
                if identifier.get("type") == "view" and in_dict(("space", "externalId", "version"), identifier):
                    yield (
                        ViewCRUD,
                        ViewReference(
                            space=identifier["space"],
                            external_id=identifier["externalId"],
                            version=str(v) if (v := identifier.get("version")) else "",
                        ),
                    )
                elif identifier.get("type") == "container" and in_dict(("space", "externalId"), identifier):
                    yield (
                        ContainerCRUD,
                        ContainerReference(space=identifier["space"], external_id=identifier["externalId"]),
                    )

        for key in ["startNode", "endNode", "type"]:
            if node_ref := item.get(key):
                if isinstance(node_ref, dict) and in_dict(("space", "externalId"), node_ref):
                    yield NodeCRUD, TypedNodeIdentifier(space=node_ref["space"], external_id=node_ref["externalId"])

    def dump_resource(self, resource: EdgeResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        # CDF resource does not have properties set, so we need to do a lookup
        local = local or {}
        sources = [
            TypedViewReference._load(source["source"]) for source in local.get("sources", []) if "source" in source
        ]

        # Default dump
        dumped = resource.as_request_resource().dump()
        if sources:
            try:
                node_id = resource.as_id()
                res = self.client.tool.instances.retrieve([node_id], source=sources[0])
            except ToolkitAPIError:
                ...
            else:
                if res:
                    # Dump again with properties from the source view.
                    dumped = res[0].as_request_resource().dump()

        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        if dumped.get("instanceType") == "edge" and "instanceType" not in local:
            # Toolkit uses file suffix to determine instanceType, so we need to remove it from the CDF resource
            # to match the local resource.
            dumped.pop("instanceType", None)

        return dumped

    def create(self, items: Sequence[EdgeRequest]) -> list[InstanceSlimDefinition]:
        return self.client.tool.instances.create(list(items))

    def retrieve(self, ids: SequenceNotStr[TypedEdgeIdentifier]) -> list[EdgeResponse]:
        results = self.client.tool.instances.retrieve(list(ids))
        return [r for r in results if isinstance(r, EdgeResponse)]

    def update(self, items: Sequence[EdgeRequest]) -> list[InstanceSlimDefinition]:
        return self.client.tool.instances.create(list(items))

    def delete(self, ids: SequenceNotStr[TypedEdgeIdentifier]) -> int:
        try:
            deleted = self.client.tool.instances.delete(list(ids))
        except ToolkitAPIError as e:
            if "not exist" in str(e) and "space" in str(e).lower():
                return 0
            raise e
        return len(deleted)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[EdgeResponse]:
        filter_ = InstanceFilter(
            instance_type="edge",
            space=[space] if space else None,
        )
        for batch in self.client.tool.instances.iterate(filter=filter_):
            for inst in batch:
                if isinstance(inst, EdgeResponse):
                    yield inst

    def count(self, ids: SequenceNotStr[TypedEdgeIdentifier]) -> int:
        return len(ids)

    def drop_data(self, ids: SequenceNotStr[TypedEdgeIdentifier]) -> int:
        # Edges will be deleted in .delete call.
        return 0

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("sources",):
            return diff_list_identifiable(local, cdf, get_identifier=lambda x: dm_identifier(x["source"]))
        return super().diff_list(local, cdf, json_path)
