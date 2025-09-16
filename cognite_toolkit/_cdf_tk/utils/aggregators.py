from abc import ABC, abstractmethod
from functools import lru_cache
from typing import ClassVar, Generic, Literal, TypeVar

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

T_CogniteFilter = TypeVar(
    "T_CogniteFilter",
    bound=AssetFilter | EventFilter | FileMetadataFilter | TimeSeriesFilter | SequenceFilter,
    contravariant=True,
)


class AssetCentricAggregator(ABC):
    _transformation_destination: ClassVar[tuple[str, ...]]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @property
    @abstractmethod
    def display_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
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
        """Converts a list of results to a unique list of integers.

        This method does the following:
        * Converts each item in the results to an integer, if possible.
        * Filters out None.
        * Removes duplicates

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
                int_id = int(id_)
            except (ValueError, TypeError):
                continue
            if int_id not in seen:
                ids.append(int_id)
                seen.add(int_id)
        return ids


class MetadataAggregator(AssetCentricAggregator, ABC, Generic[T_CogniteFilter]):
    filter_cls: type[T_CogniteFilter]

    def __init__(
        self, client: ToolkitClient, resource_name: Literal["assets", "events", "files", "timeseries", "sequences"]
    ) -> None:
        super().__init__(client)
        self.resource_name = resource_name

    def metadata_key_count(
        self, hierarchy: str | list[str] | None = None, data_sets: str | list[str] | None = None
    ) -> int:
        """Returns the number of metadata keys used by the resource."""
        return len(self.used_metadata_keys(hierarchy=hierarchy, data_sets=data_sets))

    def used_metadata_keys(
        self, hierarchy: str | list[str] | None = None, data_sets: str | list[str] | None = None
    ) -> list[tuple[str, int]]:
        """Returns a list of metadata keys and their counts."""
        hierarchy_ids, data_set_ids = self._lookup_hierarchy_data_set_pair(hierarchy, data_sets)
        return self._used_metadata_keys(hierarchy=hierarchy_ids, data_sets=data_set_ids)

    @lru_cache(maxsize=1)
    def _used_metadata_keys(
        self, hierarchy: tuple[int, ...] | None = None, data_sets: tuple[int, ...] | None = None
    ) -> list[tuple[str, int]]:
        return metadata_key_counts(
            self.client,
            self.resource_name,
            hierarchies=list(hierarchy) if hierarchy else None,
            data_sets=list(data_sets) if data_sets else None,
        )

    def _lookup_hierarchy_data_set_pair(
        self, hierarchy: str | list[str] | None = None, data_sets: str | list[str] | None = None
    ) -> tuple[tuple[int, ...] | None, tuple[int, ...] | None]:
        """Returns a tuple of hierarchy and data sets."""
        hierarchy_ids: tuple[int, ...] | None = None
        if isinstance(hierarchy, str):
            asset_id = self.client.lookup.assets.id(external_id=hierarchy, allow_empty=False)
            hierarchy_ids = (asset_id,)
        elif isinstance(hierarchy, list) and all(isinstance(item, str) for item in hierarchy):
            hierarchy_ids = tuple(sorted(self.client.lookup.assets.id(external_id=hierarchy, allow_empty=False)))

        data_set_ids: tuple[int, ...] | None = None
        if isinstance(data_sets, str):
            data_set_id = self.client.lookup.data_sets.id(external_id=data_sets, allow_empty=False)
            data_set_ids = (data_set_id,)
        elif isinstance(data_sets, list) and all(isinstance(item, str) for item in data_sets):
            data_set_ids = tuple(sorted(self.client.lookup.data_sets.id(external_id=data_sets, allow_empty=False)))

        return hierarchy_ids, data_set_ids

    @classmethod
    def create_filter(
        cls,
        hierarchy: str | list[str] | tuple[str, ...] | None = None,
        data_set_external_id: str | list[str] | tuple[str, ...] | None = None,
    ) -> T_CogniteFilter | None:
        """Creates a filter for the resource based on hierarchy and data set external ID."""
        if cls._is_empty(hierarchy) and cls._is_empty(data_set_external_id):
            return None
        asset_subtree_ids: list[dict[str, str]] | None = None
        if isinstance(hierarchy, str):
            asset_subtree_ids = [{"externalId": hierarchy}]
        elif isinstance(hierarchy, list | tuple) and hierarchy:
            asset_subtree_ids = [{"externalId": item} for item in hierarchy]
        data_set_ids: list[dict[str, str]] | None = None
        if isinstance(data_set_external_id, str):
            data_set_ids = [{"externalId": data_set_external_id}]
        elif isinstance(data_set_external_id, list | tuple) and data_set_external_id:
            data_set_ids = [{"externalId": item} for item in data_set_external_id]

        # MyPy fails to understand that filter_cls() produce a T_CogniteFilter
        return cls.filter_cls(asset_subtree_ids=asset_subtree_ids, data_set_ids=data_set_ids)  # type: ignore[return-value]

    @classmethod
    def _is_empty(cls, items: str | list[str] | tuple[str, ...] | None) -> bool:
        """Checks if the provided items are empty."""
        if items is None:
            return True
        if isinstance(items, list | tuple):
            return not items
        return False


class LabelAggregator(MetadataAggregator, ABC, Generic[T_CogniteFilter]):
    def label_count(self, hierarchy: str | list[str] | None = None, data_sets: str | list[str] | None = None) -> int:
        """Returns the number of labels used by the resource."""
        return len(self.used_labels(hierarchy=hierarchy, data_sets=data_sets))

    def used_labels(
        self, hierarchy: str | list[str] | None = None, data_sets: str | list[str] | None = None
    ) -> list[tuple[str, int]]:
        """Returns a list of labels and their counts."""
        hierarchy_ids, data_set_ids = self._lookup_hierarchy_data_set_pair(hierarchy, data_sets)
        return self._used_labels(hierarchy=hierarchy_ids, data_sets=data_set_ids)

    @lru_cache(maxsize=1)
    def _used_labels(
        self, hierarchy: tuple[int, ...] | None = None, data_sets: tuple[int, ...] | None = None
    ) -> list[tuple[str, int]]:
        """Returns a list of labels and their counts."""
        return label_count(
            self.client,
            self.resource_name,
            hierarchies=list(hierarchy) if hierarchy else None,
            data_sets=list(data_sets) if data_sets else None,
        )


class AssetAggregator(LabelAggregator[AssetFilter]):
    _transformation_destination = ("assets", "asset_hierarchy")
    filter_cls = AssetFilter

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "assets")

    @property
    def display_name(self) -> str:
        return "Assets"

    def count(
        self,
        hierarchy: str | list[str] | tuple[str, ...] | None = None,
        data_set_external_id: str | list[str] | tuple[str, ...] | None = None,
    ) -> int:
        return self.client.assets.aggregate_count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.assets.aggregate_unique_values(
            AssetProperty.data_set_id, filter=self.create_filter(hierarchy)
        )
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


class EventAggregator(MetadataAggregator[EventFilter]):
    _transformation_destination = ("events",)
    filter_cls = EventFilter

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "events")

    @property
    def display_name(self) -> str:
        return "Events"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.events.aggregate_count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.events.aggregate_unique_values(
            property=EventProperty.data_set_id, filter=self.create_filter(hierarchy)
        )
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


class FileAggregator(LabelAggregator[FileMetadataFilter]):
    _transformation_destination = ("files",)
    filter_cls = FileMetadataFilter

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "files")

    @property
    def display_name(self) -> str:
        return "Files"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        response = self.client.files.aggregate(filter=self.create_filter(hierarchy, data_set_external_id))
        if response:
            return response[0].count
        else:
            return 0

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


class TimeSeriesAggregator(MetadataAggregator[TimeSeriesFilter]):
    _transformation_destination = ("timeseries",)
    filter_cls = TimeSeriesFilter

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "timeseries")

    @property
    def display_name(self) -> str:
        return "TimeSeries"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.time_series.aggregate_count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.time_series.aggregate_unique_values(
            property=TimeSeriesProperty.data_set_id, filter=self.create_filter(hierarchy)
        )
        ids = self._to_unique_int_list(results.unique)
        return self.client.lookup.data_sets.external_id(ids)


class SequenceAggregator(MetadataAggregator):
    _transformation_destination = ("sequences",)
    filter_cls = SequenceFilter

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "sequences")

    @property
    def display_name(self) -> str:
        return "Sequences"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.sequences.aggregate_count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.sequences.aggregate_unique_values(
            property=SequenceProperty.data_set_id, filter=self.create_filter(hierarchy)
        )
        ids = self._to_unique_int_list(results.unique)
        return self.client.lookup.data_sets.external_id(ids)


class RelationshipAggregator(AssetCentricAggregator):
    _transformation_destination = ("relationships",)

    @property
    def display_name(self) -> str:
        return "Relationships"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
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

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        if hierarchy is not None or data_set_external_id is not None:
            raise NotImplementedError()
        return label_aggregate_count(self.client)

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        raise NotImplementedError()
