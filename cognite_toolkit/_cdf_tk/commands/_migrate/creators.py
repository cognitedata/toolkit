from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic, cast

from cognite.client.data_classes import DataSetList, filters
from cognite.client.data_classes.aggregations import UniqueResult
from cognite.client.data_classes.assets import AssetProperty
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeOrEdgeData,
    SpaceApply,
    ViewId,
)

from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.events import EventProperty

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APM_CONFIG_SPACE, APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeReference,
    NodeRequest,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import TypedNodeIdentifier
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import LocationFilterRequest
from cognite_toolkit._cdf_tk.cruds import NodeCRUD, ResourceCRUD, SpaceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID

from .data_model import CREATED_SOURCE_SYSTEM_VIEW_ID, SPACE, SPACE_SOURCE_VIEW_ID


@dataclass
class CreatedResource(Generic[T_ResourceRequest]):
    resource: T_ResourceRequest
    config_data: dict[str, Any] | None = None
    filestem: str | None = None


@dataclass
class ToCreateResources(Generic[T_ID, T_ResourceRequest, T_ResourceResponse]):
    resources: Sequence[CreatedResource[T_ResourceRequest]]
    crud_cls: type[ResourceCRUD[T_ID, T_ResourceRequest, T_ResourceResponse]]
    display_name: str
    store_linage: Callable[[], int] | None = None


class MigrationCreator(ABC):
    """Base class for migration resources configurations that are created resources."""

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def create_resources(self) -> Iterable[ToCreateResources]:
        raise NotImplementedError("Subclasses should implement this method")


class InstanceSpaceCreator(MigrationCreator):
    """Creates instance spaces for migration."""

    def __init__(
        self, client: ToolkitClient, datasets: DataSetList | None = None, data_set_external_ids: list[str] | None = None
    ) -> None:
        super().__init__(client)
        if sum([datasets is not None, data_set_external_ids is not None]) != 1:
            raise ValueError("Exactly one of datasets or data_set_external_ids must be provided.")
        self.data_set_external_ids = data_set_external_ids
        self.datasets = datasets or DataSetList([])

    def create_resources(self) -> Iterable[ToCreateResources]:
        if self.data_set_external_ids is not None:
            self.datasets = self.client.data_sets.retrieve_multiple(external_ids=self.data_set_external_ids)

        if missing_external_ids := [ds.id for ds in self.datasets if ds.external_id is None]:
            raise ToolkitRequiredValueError(
                f"Cannot create instance spaces for datasets with missing external IDs: {humanize_collection(missing_external_ids)}"
            )
        data_set_by_external_id = {ds.external_id: ds for ds in self.datasets}
        created_resources: list[CreatedResource[SpaceApply]] = []
        linage_nodes: list[NodeRequest] = []
        for dataset in self.datasets:
            space = SpaceApply(
                # This is checked above
                space=dataset.external_id,  # type: ignore[arg-type]
                name=dataset.name,
                description=dataset.description,
            )
            linage_node = NodeRequest(
                space=SPACE.space,
                external_id=space.space,
                sources=[
                    InstanceSource(
                        source=ViewReference(
                            space=SPACE_SOURCE_VIEW_ID.space,
                            external_id=SPACE_SOURCE_VIEW_ID.external_id,
                            version=cast(str, SPACE_SOURCE_VIEW_ID.version),
                        ),
                        properties={
                            "instanceSpace": space.space,
                            "dataSetId": data_set_by_external_id[space.space].id,
                            "dataSetExternalId": data_set_by_external_id[space.space].external_id,
                        },
                    )
                ],
            )
            linage_nodes.append(linage_node)
            created_resources.append(
                CreatedResource(
                    resource=space,
                    config_data=space.dump(),
                    filestem=space.space,
                )
            )
        yield ToCreateResources(
            resources=created_resources,
            crud_cls=SpaceCRUD,
            display_name="Instance Space",
            store_linage=lambda: self.store_lineage(linage_nodes),
        )

    def store_lineage(self, lineage_nodes: Sequence[NodeRequest]) -> int:
        res = self.client.tool.instances.create(lineage_nodes)
        return len(res)


class SourceSystemCreator(MigrationCreator):
    COGNITE_SOURCE_SYSTEM_VIEW_ID = ViewId("cdf_cdm", "CogniteSourceSystem", "v1")

    def __init__(
        self,
        client: ToolkitClient,
        instance_space: str,
        data_set_external_id: str | None = None,
        hierarchy: str | None = None,
    ) -> None:
        super().__init__(client)
        if sum([data_set_external_id is not None, hierarchy is not None]) != 1:
            raise ValueError("Exactly one of data_set_external_id or hierarchy must be provided.")
        self._instance_space = instance_space
        self.data_set_external_id = data_set_external_id
        self.hierarchy = hierarchy

    def create_resources(self) -> Iterable[ToCreateResources]:
        existing_resources = self._get_existing_source_systems()
        seen: set[str] = set()
        to_create: list[CreatedResource[NodeApply]] = []
        for source in self._lookup_sources():
            source_str = source.value
            if not isinstance(source_str, str) or source_str in seen:
                continue
            seen.add(source_str)
            if existing_id := existing_resources.get(source_str):
                self.client.console.print(f"Skipping {source_str} as it already exists in {existing_id!r}.")
                continue
            data_source = NodeOrEdgeData(source=self.COGNITE_SOURCE_SYSTEM_VIEW_ID, properties={"name": source_str})
            linage_source = NodeOrEdgeData(source=CREATED_SOURCE_SYSTEM_VIEW_ID, properties={"source": source_str})
            to_create.append(
                CreatedResource[NodeApply](
                    resource=NodeApply(
                        space=self._instance_space,
                        external_id=source_str,
                        sources=[data_source, linage_source],
                    ),
                    config_data=NodeApply(
                        space=self._instance_space,
                        external_id=source_str,
                        sources=[data_source],
                    ).dump(),
                    filestem=source_str,
                )
            )
        yield ToCreateResources(
            resources=to_create,
            crud_cls=NodeCRUD,
            display_name="Source System",
            store_linage=lambda: len(to_create),
        )

    def _get_existing_source_systems(self) -> dict[str, NodeReference]:
        all_existing = self.client.migration.created_source_system.list(limit=-1)
        return {node.source: NodeReference(space=node.space, external_id=node.external_id) for node in all_existing}

    def _lookup_sources(self) -> Iterable[UniqueResult]:
        yield from self.client.assets.aggregate_unique_values(AssetProperty.source, filter=self._simple_filter)
        yield from self.client.events.aggregate_unique_values(property=EventProperty.source, filter=self._simple_filter)
        yield from self.client.documents.aggregate_unique_values(
            SourceFileProperty.source, filter=self._advanced_filter, limit=1000
        )

    @cached_property
    def _simple_filter(self) -> dict[str, Any] | None:
        if self.data_set_external_id is not None:
            return {"dataSetIds": [{"externalId": self.data_set_external_id}]}
        if self.hierarchy is not None:
            return {"assetSubtreeIds": [{"externalId": self.hierarchy}]}
        return None

    @cached_property
    def _advanced_filter(self) -> filters.Filter:
        if self.data_set_external_id is not None:
            data_set_id = self.client.lookup.data_sets.id(self.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(f"Data set with external ID '{self.data_set_external_id}' not found.")
            return filters.Equals(SourceFileProperty.data_set_id, data_set_id)
        if self.hierarchy is not None:
            return filters.InAssetSubtree(SourceFileProperty.asset_external_ids, [self.hierarchy])
        else:
            raise ValueError("This should not happen.")


class InfieldV2ConfigCreator(MigrationCreator):
    def __init__(
        self,
        client: ToolkitClient,
        external_ids: Sequence[str] | None = None,
        apm_configs: Sequence[APMConfigResponse] | None = None,
    ) -> None:
        super().__init__(client)
        if sum([external_ids is not None, apm_configs is not None]) != 1:
            raise ValueError("Exactly one of external_ids or apm_configs must be provided.")
        self.external_ids = external_ids
        self.apm_configs = apm_configs

    def create_resources(self) -> Iterable[ToCreateResources]:
        if self.external_ids is not None:
            apm_configs = self.client.infield.apm_config.retrieve(
                TypedNodeIdentifier.from_str_ids(self.external_ids, space=APM_CONFIG_SPACE)
            )
        else:
            apm_configs = list(self.apm_configs)

        all_location_configs: list[CreatedResource[InFieldCDMLocationConfigRequest]] = []
        all_location_filters: list[CreatedResource[InFieldCDMLocationConfigRequest]] = []
        for apm_config in apm_configs:
            location_configs, location_filters = self._create_infield_v2_config(apm_config)
            all_location_configs.extend(
                CreatedResource(
                    resource=loc_config,
                    config_data=loc_config.dump(),
                    filestem=f"{apm_config.external_id}_location_{loc_config.name}",
                )
                for idx, loc_config in enumerate(location_configs)

            )
            all_location_filters.extend(
                CreatedResource(
                    resource=loc_filter,
                    config_data=loc_filter.dump(),
                    filestem=f"{apm_config.external_id}_filter_{loc_filter.name}",
                )
                for idx, loc_filter in enumerate(location_filters)
            )
        yield ToCreateResources(
            resources=all_location_configs,
            crud_cls=NodeCRUD,



    def _create_infield_v2_config(
        self, config: APMConfigResponse
    ) -> tuple[list[InFieldCDMLocationConfigRequest], list[LocationFilterRequest]]:
        location_configs: list[InFieldCDMLocationConfigRequest] = []
        location_filters: list[LocationFilterRequest] = []