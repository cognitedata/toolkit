from abc import ABC, abstractmethod
from typing import ClassVar, Literal

from cognite.client.data_classes import (
    AssetFilter,
    EventFilter,
    FileMetadataFilter,
    SequenceFilter,
    TimeSeriesFilter,
    Transformation,
    filters,
)
from cognite.client.data_classes.assets import AssetProperty
from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.events import EventProperty
from cognite.client.data_classes.sequences import SequenceProperty
from cognite.client.data_classes.time_series import TimeSeriesProperty

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import (
    label_aggregate_count,
    label_count,
    metadata_key_counts,
    relationship_aggregate_count,
)
from cognite_toolkit._cdf_tk.utils.sql_parser import SQLParser


class AssetCentricAggregator(ABC):
    _transformation_destination: ClassVar[tuple[str, ...]]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @property
    @abstractmethod
    def display_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        raise NotImplementedError

    def transformation_count(self) -> int:
        """Returns the number of transformations associated with the resource."""
        transformation_count = 0
        for destination in self._transformation_destination:
            for chunk in self.client.transformations(chunk_size=1000, destination_type=destination, limit=None):
                transformation_count += len(chunk)
        return transformation_count

    @abstractmethod
    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        raise NotImplementedError

    def used_transformations(self, data_set_external_ids: list[str]) -> list[Transformation]:
        """Returns a list of transformations used by the resource."""
        data_set_ids = self.client.lookup.data_sets.id(data_set_external_ids, allow_empty=True)
        found_transformations: list[Transformation] = []
        for destination in self._transformation_destination:
            for chunk in self.client.transformations(chunk_size=1000, destination_type=destination, limit=None):
                for transformation in chunk:
                    if SQLParser(transformation.query or "", operation="profiling").is_using_data_set(
                        data_set_ids, data_set_external_ids
                    ):
                        found_transformations.append(transformation)
        return found_transformations

    @staticmethod
    def _to_unique_int_list(results: list) -> list[int]:
        """Converts a list of results to a list of integers, ignoring non-integer values.

        This is used as the aggregation results are inconsistently implemented for the different resources,
        when aggregating dataSetIds, Sequences, TimeSeries, and Files return a list of strings, while
        Assets and Events return a list of integers. In addition, the files aggregation can return
        duplicated values.
        """
        seen: set[int] = set()
        ids: list[int] = []
        for id_ in results:
            if isinstance(id_, int) and id_ not in seen:
                ids.append(id_)
                seen.add(id_)
            try:
                int_id = int(id_)  # type: ignore[arg-type]
            except (ValueError, TypeError):
                continue
            if int_id not in seen:
                ids.append(int_id)
                seen.add(int_id)
        return ids


class MetadataAggregator(AssetCentricAggregator, ABC):
    def __init__(
        self, client: ToolkitClient, resource_name: Literal["assets", "events", "files", "timeseries", "sequences"]
    ) -> None:
        super().__init__(client)
        self.resource_name = resource_name

    def metadata_key_count(self, hierarchy: str | None = None, data_set_external_ids: str | None = None) -> int:
        return len(metadata_key_counts(self.client, self.resource_name))


class LabelAggregator(MetadataAggregator, ABC):
    def label_count(self) -> int:
        return len(label_count(self.client, self.resource_name))


class AssetAggregator(LabelAggregator):
    _transformation_destination = ("assets", "asset_hierarchy")

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "assets")

    @property
    def display_name(self) -> str:
        return "Assets"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        return self.client.assets.aggregate_count(filter=self._create_hierarchy_filter(hierarchy, data_set_external_id))

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> AssetFilter | None:
        if hierarchy is None:
            return None
        return AssetFilter(
            asset_subtree_ids=[{"externalId": hierarchy}],
            data_set_ids=[{"externalId": data_set_external_id}] if data_set_external_id else None,
        )

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.assets.aggregate_unique_values(
            AssetProperty.data_set_id, filter=self._create_hierarchy_filter(hierarchy)
        )
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


class EventAggregator(MetadataAggregator):
    _transformation_destination = ("events",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "events")

    @property
    def display_name(self) -> str:
        return "Events"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        return self.client.events.aggregate_count(filter=self._create_hierarchy_filter(hierarchy, data_set_external_id))

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> EventFilter | None:
        if hierarchy is None:
            return None
        return EventFilter(
            asset_subtree_ids=[{"externalId": hierarchy}] if hierarchy else None,
            data_set_ids=[{"externalId": data_set_external_id}] if data_set_external_id else None,
        )

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.events.aggregate_unique_values(
            property=EventProperty.data_set_id, filter=self._create_hierarchy_filter(hierarchy)
        )
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


class FileAggregator(LabelAggregator):
    _transformation_destination = ("files",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "files")

    @property
    def display_name(self) -> str:
        return "Files"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        response = self.client.files.aggregate(filter=self._create_hierarchy_filter(hierarchy, data_set_external_id))
        if response:
            return response[0].count
        else:
            return 0

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> FileMetadataFilter | None:
        if hierarchy is None:
            return None
        return FileMetadataFilter(
            asset_subtree_ids=[{"externalId": hierarchy}] if hierarchy else None,
            data_set_ids=[{"externalId": data_set_external_id}] if data_set_external_id else None,
        )

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        filter_: filters.Filter | None = None
        if hierarchy is not None:
            filter_ = filters.InAssetSubtree("assetExternalIds", [hierarchy])
        results = self.client.documents.aggregate_unique_values(
            property=SourceFileProperty.data_set_id, filter=filter_, limit=1000
        )
        ids = self._to_unique_int_list(results.unique)
        return self.client.lookup.data_sets.external_id(list(ids))


class TimeSeriesAggregator(MetadataAggregator):
    _transformation_destination = ("timeseries",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "timeseries")

    @property
    def display_name(self) -> str:
        return "TimeSeries"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        return self.client.time_series.aggregate_count(
            filter=self._create_hierarchy_filter(hierarchy, data_set_external_id)
        )

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> TimeSeriesFilter | None:
        if hierarchy is None:
            return None
        return TimeSeriesFilter(
            asset_subtree_ids=[{"externalId": hierarchy}] if hierarchy else None,
            data_set_ids=[{"externalId": data_set_external_id}] if data_set_external_id else None,
        )

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.time_series.aggregate_unique_values(
            property=TimeSeriesProperty.data_set_id, filter=self._create_hierarchy_filter(hierarchy)
        )
        ids = self._to_unique_int_list(results.unique)
        return self.client.lookup.data_sets.external_id(ids)


class SequenceAggregator(MetadataAggregator):
    _transformation_destination = ("sequences",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "sequences")

    @property
    def display_name(self) -> str:
        return "Sequences"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        return self.client.sequences.aggregate_count(
            filter=self._create_hierarchy_filter(hierarchy, data_set_external_id)
        )

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> SequenceFilter | None:
        if hierarchy is None:
            return None
        return SequenceFilter(
            asset_subtree_ids=[{"externalId": hierarchy}] if hierarchy else None,
            data_set_ids=[{"externalId": data_set_external_id}] if data_set_external_id else None,
        )

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.sequences.aggregate_unique_values(
            property=SequenceProperty.data_set_id, filter=self._create_hierarchy_filter(hierarchy)
        )
        ids = self._to_unique_int_list(results.unique)
        return self.client.lookup.data_sets.external_id(ids)


class RelationshipAggregator(AssetCentricAggregator):
    _transformation_destination = ("relationships",)

    @property
    def display_name(self) -> str:
        return "Relationships"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        if hierarchy is not None or data_set_external_id is not None:
            raise NotImplementedError()
        results = relationship_aggregate_count(self.client)
        return sum(result.count for result in results)

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        raise NotImplementedError()


class LabelCountAggregator(AssetCentricAggregator):
    _transformation_destination = ("labels",)

    @property
    def display_name(self) -> str:
        return "Labels"

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        if hierarchy is not None or data_set_external_id is not None:
            raise NotImplementedError()
        return label_aggregate_count(self.client)

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        raise NotImplementedError()
