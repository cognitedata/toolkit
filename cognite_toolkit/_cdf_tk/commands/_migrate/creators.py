from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic

from cognite.client.data_classes import DataSetList, filters
from cognite.client.data_classes._base import T_CogniteResourceList
from cognite.client.data_classes.aggregations import UniqueResult
from cognite.client.data_classes.assets import AssetProperty
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeApplyList,
    NodeOrEdgeData,
    SpaceApply,
    SpaceApplyList,
    ViewId,
)
from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.events import EventProperty

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import NodeCRUD, ResourceCRUD, SpaceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .data_model import CREATED_SOURCE_SYSTEM_VIEW_ID, SPACE, SPACE_SOURCE_VIEW_ID


@dataclass
class ResourceConfig:
    filestem: str
    data: dict[str, Any]


class MigrationCreator(ABC, Generic[T_CogniteResourceList]):
    """Base class for migration resources configurations that are created from asset-centric resources."""

    CRUD: type[ResourceCRUD]
    DISPLAY_NAME: str

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def create_resources(self) -> T_CogniteResourceList:
        raise NotImplementedError("Subclasses should implement this method")

    @abstractmethod
    def resource_configs(self, resources: T_CogniteResourceList) -> list[ResourceConfig]:
        raise NotImplementedError("Subclasses should implement this method")

    @abstractmethod
    def store_lineage(self, resources: T_CogniteResourceList) -> int:
        """Store lineage information for the created resources.

        Args:
            resources: The list of created resources.
        """
        raise NotImplementedError("Subclasses should implement this method")


class InstanceSpaceCreator(MigrationCreator[SpaceApplyList]):
    """Creates instance spaces for migration."""

    CRUD = SpaceCRUD
    DISPLAY_NAME = "Instance Space"

    def __init__(
        self, client: ToolkitClient, datasets: DataSetList | None = None, data_set_external_ids: list[str] | None = None
    ) -> None:
        super().__init__(client)
        if sum([datasets is not None, data_set_external_ids is not None]) != 1:
            raise ValueError("Exactly one of datasets or data_set_external_ids must be provided.")
        self.data_set_external_ids = data_set_external_ids
        self.datasets = datasets or DataSetList([])

    def create_resources(self) -> SpaceApplyList:
        if self.data_set_external_ids is not None:
            self.datasets = self.client.data_sets.retrieve_multiple(external_ids=self.data_set_external_ids)

        if missing_external_ids := [ds.id for ds in self.datasets if ds.external_id is None]:
            raise ToolkitRequiredValueError(
                f"Cannot create instance spaces for datasets with missing external IDs: {humanize_collection(missing_external_ids)}"
            )

        return SpaceApplyList(
            [
                SpaceApply(
                    # This is checked above
                    space=dataset.external_id,  # type: ignore[arg-type]
                    name=dataset.name,
                    description=dataset.description,
                )
                for dataset in self.datasets
            ]
        )

    def resource_configs(self, resources: SpaceApplyList) -> list[ResourceConfig]:
        return [ResourceConfig(filestem=space.space, data=space.dump()) for space in resources]

    def store_lineage(self, resources: SpaceApplyList) -> int:
        data_set_by_external_id = {ds.external_id: ds for ds in self.datasets}
        nodes = NodeApplyList(
            [
                NodeApply(
                    space=SPACE.space,
                    external_id=space.space,
                    sources=[
                        NodeOrEdgeData(
                            source=SPACE_SOURCE_VIEW_ID,
                            properties={
                                "instanceSpace": space.space,
                                "dataSetId": data_set_by_external_id[space.space].id,
                                "dataSetExternalId": data_set_by_external_id[space.space].external_id,
                            },
                        )
                    ],
                )
                for space in resources
            ]
        )
        res = self.client.data_modeling.instances.apply(nodes)
        return len(res.nodes)


class SourceSystemCreator(MigrationCreator[NodeApplyList]):
    CRUD = NodeCRUD
    DISPLAY_NAME = "Source System"
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

    def create_resources(self) -> NodeApplyList:
        seen: set[str] = set()
        nodes = NodeApplyList([])
        for source in self._lookup_sources():
            source_str = source.value
            if not isinstance(source_str, str) or source_str in seen:
                continue
            seen.add(source_str)
            nodes.append(
                NodeApply(
                    space=self._instance_space,
                    external_id=source_str,
                    sources=[
                        NodeOrEdgeData(source=self.COGNITE_SOURCE_SYSTEM_VIEW_ID, properties={"name": source_str}),
                        NodeOrEdgeData(source=CREATED_SOURCE_SYSTEM_VIEW_ID, properties={"source": source_str}),
                    ],
                )
            )
        return nodes

    def resource_configs(self, resources: NodeApplyList) -> list[ResourceConfig]:
        output: list[ResourceConfig] = []
        for node in resources:
            copy = NodeApply(
                space=node.space,
                external_id=node.external_id,
                # We remove the lineage source as this is not expected to be part of the governed
                # SourceSystem.
                sources=[s for s in node.sources if s.source != CREATED_SOURCE_SYSTEM_VIEW_ID],
            )
            output.append(ResourceConfig(filestem=node.external_id, data=copy.dump()))
        return output

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
            return filters.Equals(
                SourceFileProperty.data_set_id, self.client.lookup.data_sets.id(self.data_set_external_id)
            )
        if self.hierarchy is not None:
            return filters.InAssetSubtree(SourceFileProperty.asset_external_ids, [self.hierarchy])
        else:
            raise ValueError("This should not happen.")

    def store_lineage(self, resources: NodeApplyList) -> int:
        # We already store lineage when creating the resources.
        return len(resources)
