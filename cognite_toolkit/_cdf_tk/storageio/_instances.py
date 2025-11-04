from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import ClassVar

from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    ContainerList,
    EdgeApply,
    MappedProperty,
    NodeApply,
    SpaceList,
    ViewId,
    ViewList,
)
from cognite.client.data_classes.data_modeling.instances import Instance, InstanceApply
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceList
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, SpaceCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import ConfigurableStorageIO, Page, UploadableStorageIO, UploadItem
from .selectors import InstanceFileSelector, InstanceSelector, InstanceSpaceSelector, InstanceViewSelector


class InstanceIO(
    ConfigurableStorageIO[InstanceSelector, Instance],
    UploadableStorageIO[InstanceSelector, Instance, InstanceApply],
):
    """This class provides functionality to interact with instances in Cognite Data Fusion (CDF).

    It is used to download, upload, and purge instances, as well as spaces,views, and containers related to instances.

    Args:
        client (ToolkitClient): An instance of ToolkitClient to interact with the CDF API.
        remove_existing_version (bool): Whether to remove existing versions of instances during upload.
            Default is True. Existing versions are used to safeguard against accidental overwrites.

    """

    KIND = "Instances"
    DISPLAY_NAME = "Instances"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = "/models/instances"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = MappingProxyType({"autoCreateDirectRelations": True})
    BASE_SELECTOR = InstanceSelector

    def __init__(self, client: ToolkitClient, remove_existing_version: bool = True) -> None:
        super().__init__(client)
        self._remove_existing_version = remove_existing_version
        # Cache for view to restricted properties mapping
        self._view_restricted_properties_cache: dict[ViewId, set[str]] = {}

    def as_id(self, item: Instance) -> str:
        return f"{item.space}:{item.external_id}"

    def _get_restricted_properties_for_view(self, view_id: ViewId) -> set[str]:
        """Get the set of restricted property names for a given view.

        This checks if the view writes to the CogniteAsset container in the cdf_cdm space,
        and if so, returns the view properties that should be filtered out during upload.

        The CogniteAsset container has managed properties for asset hierarchy that should not
        be directly written:
        - pathLastUpdatedTime (maps to assetHierarchy_path_last_updated_time)
        - path (maps to assetHierarchy_path)
        - root (maps to assetHierarchy_root)

        Args:
            view_id: The ViewId to check

        Returns:
            Set of property names that should be filtered from upload data
        """
        # Return cached result if available
        if view_id in self._view_restricted_properties_cache:
            return self._view_restricted_properties_cache[view_id]

        restricted_properties: set[str] = set()

        # Container properties that are read-only should not be uploaded.
        RESTRICTED_CONTAINER_PROPERTIES = {
            ContainerId(space="cdf_cdm", external_id="CogniteAsset"): {
                "assetHierarchy_path_last_updated_time",
                "assetHierarchy_path",
                "assetHierarchy_root",
            },
            ContainerId(space="cdf_cdm", external_id="CogniteFile"): {"isUploaded"},
        }

        # Retrieve the view to check its properties
        view = self.client.data_modeling.views.retrieve(view_id)
        if view is None:
            # Cache empty result
            self._view_restricted_properties_cache[view_id] = restricted_properties
            return restricted_properties
        view = view[-1]

        # Check each property in the view
        for property_identifier, property in view.properties.items():
            if isinstance(
                property, MappedProperty
            ) and property.container_property_identifier in RESTRICTED_CONTAINER_PROPERTIES.get(
                property.container, set()
            ):
                restricted_properties.add(property_identifier)

        # Cache the result
        self._view_restricted_properties_cache[view_id] = restricted_properties
        return restricted_properties

    def _filter_restricted_properties(self, item_json: dict[str, JsonVal], selector: InstanceSelector | None) -> None:
        """
        Filter out restricted properties from the item JSON based on the selector's view.

        Args:
            item_json: The item data as a JSON dictionary
            selector: The selector containing view information
        """
        if (
            selector is None
            or not isinstance(selector, InstanceViewSelector | InstanceSpaceSelector)
            or selector.view is None
        ):
            return

        restricted_properties = self._get_restricted_properties_for_view(selector.view.as_id())

        if not restricted_properties:
            return

        # Filter out restricted properties from the sources
        for source in item_json.get("sources", []):
            if isinstance(source, dict) and "properties" in source and isinstance(source["properties"], dict):
                for restricted_prop in restricted_properties:
                    source["properties"].pop(restricted_prop, None)

    def stream_data(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[Page]:
        if isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            chunk = InstanceList([])
            total = 0
            for instance in iterate_instances(client=self.client, **selector.as_filter_args()):
                if limit is not None and total >= limit:
                    break
                total += 1
                chunk.append(instance)
                if len(chunk) >= self.CHUNK_SIZE:
                    yield Page(worker_id="main", items=chunk)
                    chunk = InstanceList([])
            if chunk:
                yield Page(worker_id="main", items=chunk)
        elif isinstance(selector, InstanceFileSelector):
            for node_chunk in chunker_sequence(selector.node_ids, self.CHUNK_SIZE):
                yield Page(
                    worker_id="main",
                    items=InstanceList(self.client.data_modeling.instances.retrieve(nodes=node_chunk).nodes),
                )
            for edge_chunk in chunker_sequence(selector.edge_ids, self.CHUNK_SIZE):
                yield Page(
                    worker_id="main",
                    items=InstanceList(self.client.data_modeling.instances.retrieve(edges=edge_chunk).edges),
                )
        else:
            raise NotImplementedError()

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[list[InstanceId]]:
        if isinstance(selector, InstanceFileSelector) and selector.validate_instance is False:
            instances_to_yield = selector.instance_ids
            if limit is not None:
                instances_to_yield = instances_to_yield[:limit]
            yield from chunker_sequence(instances_to_yield, self.CHUNK_SIZE)
        else:
            yield from ([instance.as_id() for instance in chunk.items] for chunk in self.stream_data(selector, limit))

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector) or (
            isinstance(selector, InstanceSpaceSelector) and selector.view
        ):
            result = self.client.data_modeling.instances.aggregate(
                # MyPy do not understand that selector.view is always defined here.
                view=selector.view.as_id(),  # type: ignore[union-attr]
                aggregates=Count("externalId"),
                instance_type=selector.instance_type,
                space=selector.get_instance_spaces(),
            )
            return int(result.value or 0)
        elif isinstance(selector, InstanceSpaceSelector):
            statistics = self.client.data_modeling.statistics.spaces.retrieve(space=selector.instance_space)
            if statistics is None:
                return None
            if selector.instance_type == "node":
                return statistics.nodes
            elif selector.instance_type == "edge":
                return statistics.edges
            # This should never happen due to validation in the selector.
            raise ValueError(f"Unknown instance type {selector.instance_type!r}")
        elif isinstance(selector, InstanceFileSelector):
            return len(selector.instance_ids)
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[Instance], selector: InstanceSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        return [instance.as_write().dump(camel_case=True) for instance in data_chunk]

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]], selector: InstanceSelector | None = None
    ) -> Sequence[UploadItem[InstanceApply]]:
        """Convert a JSON-compatible chunk of data back to a writable Cognite resource list.

        This override adds support for filtering restricted properties based on the selector's view.

        Args:
            data_chunk: A list of tuples, each containing a source ID and a dictionary representing
                the data in a JSON-compatible format.
            selector: Optional selection criteria to identify where to upload the data.
        Returns:
            A writable Cognite resource list representing the data.
        """
        result: list[UploadItem[InstanceApply]] = []
        for source_id, item_json in data_chunk:
            # Filter out restricted properties if applicable
            self._filter_restricted_properties(item_json, selector)
            item = self.json_to_resource(item_json)
            result.append(UploadItem(source_id=source_id, item=item))
        return result

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceApply:
        # There is a bug in the SDK where InstanceApply._load turns all keys to snake_case.
        # So we cannot use InstanceApplyList._load here.
        instance_type = item_json.get("instanceType")
        item_to_load = dict(item_json)
        if self._remove_existing_version and "existingVersion" in item_to_load:
            del item_to_load["existingVersion"]
        if instance_type == "node":
            return NodeApply._load(item_to_load, cognite_client=self.client)
        elif instance_type == "edge":
            return EdgeApply._load(item_to_load, cognite_client=self.client)
        else:
            raise ValueError(f"Unknown instance type {instance_type!r}")

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        if not isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            return
        spaces = list(set((selector.get_instance_spaces() or []) + (selector.get_schema_spaces() or [])))
        if not spaces:
            return
        space_crud = SpaceCRUD.create_loader(self.client)
        retrieved_spaces = space_crud.retrieve(spaces)
        retrieved_spaces = SpaceList([space for space in retrieved_spaces if not space.is_global])
        if not retrieved_spaces:
            return
        for space in retrieved_spaces:
            yield StorageIOConfig(
                kind=SpaceCRUD.kind,
                folder_name=SpaceCRUD.folder_name,
                value=space_crud.dump_resource(space),
                filename=sanitize_filename(space.space),
            )
        if not selector.view:
            return
        view_id = selector.view.as_id()
        view_crud = ViewCRUD(self.client, None, None, topological_sort_implements=True)
        views = view_crud.retrieve([view_id])
        views = ViewList([view for view in views if not view.is_global])
        if not views:
            return
        for view in views:
            filename = f"{view.space}_{view.external_id}"
            if view.version is not None:
                filename += f"_{view.version}"
            yield StorageIOConfig(
                kind=ViewCRUD.kind,
                folder_name=ViewCRUD.folder_name,
                value=view_crud.dump_resource(view),
                filename=sanitize_filename(filename),
            )
        container_ids = list({container for view in views for container in view.referenced_containers() or []})
        if not container_ids:
            return
        container_crud = ContainerCRUD.create_loader(self.client)
        containers = container_crud.retrieve(container_ids)
        containers = ContainerList([container for container in containers if not container.is_global])
        if not containers:
            return
        for container in containers:
            yield StorageIOConfig(
                kind=ContainerCRUD.kind,
                folder_name=ContainerCRUD.folder_name,
                value=container_crud.dump_resource(container),
                filename=sanitize_filename(f"{container.space}_{container.external_id}"),
            )
