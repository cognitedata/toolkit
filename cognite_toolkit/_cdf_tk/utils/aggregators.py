from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.utils.cdf import (
    label_aggregate_count,
    relationship_aggregate_count,
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

    @abstractmethod
    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        raise NotImplementedError

    @staticmethod
    def _to_unique_int_list(results: list[Any]) -> list[int]:
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

    def _to_dataset_id(self, data_set_external_id: str | list[str] | None) -> list[int] | None:
        """Converts data set external IDs to data set IDs."""
        dataset_id: list[int] | None = None
        if data_set_external_id is not None:
            if isinstance(data_set_external_id, str):
                data_set_external_id = [data_set_external_id]
            dataset_id = self.client.lookup.data_sets.id(data_set_external_id, allow_empty=False)
        return dataset_id

    def _data_set_external_ids(self, values: list[Any], *, coerce: bool) -> list[str]:
        if coerce:
            ids = self._to_unique_int_list(values)
        else:
            ids = [value for value in values if isinstance(value, int)]
        return self.client.lookup.data_sets.external_id(ids)


class MetadataAggregator(AssetCentricAggregator, ABC):
    def __init__(
        self, client: ToolkitClient, resource_name: Literal["assets", "events", "files", "timeseries", "sequences"]
    ) -> None:
        super().__init__(client)
        self.resource_name = resource_name

    def _lookup_hierarchy_data_set_pair(
        self, hierarchy: str | list[str] | None, data_sets: str | list[str] | None, operation: str
    ) -> tuple[tuple[int, ...] | None, tuple[int, ...] | None]:
        """Returns a tuple of hierarchy and data sets."""
        hierarchy_ids: tuple[int, ...] | None = None
        if isinstance(hierarchy, str):
            asset_id = self.client.lookup.assets.id(external_id=hierarchy, allow_empty=False)
            if asset_id is None:
                raise ToolkitMissingResourceError(f"Cannot {operation}. Asset with external ID {hierarchy!r} not found")
            hierarchy_ids = (asset_id,)
        elif isinstance(hierarchy, list) and all(isinstance(item, str) for item in hierarchy):
            asset_ids = self.client.lookup.assets.id(external_id=hierarchy, allow_empty=False)
            if len(asset_ids) != len(hierarchy):
                missing = set(hierarchy) - set(
                    self.client.lookup.assets.external_id([id_ for id_ in asset_ids if id_ is not None])
                )
                raise ToolkitMissingResourceError(
                    f"Cannot {operation}. Assets with external IDs {sorted(missing)!r} not found"
                )
            hierarchy_ids = tuple(sorted(asset_ids))

        data_set_ids: tuple[int, ...] | None = None
        if isinstance(data_sets, str):
            data_set_id = self.client.lookup.data_sets.id(external_id=data_sets, allow_empty=False)
            if data_set_id is None:
                raise ToolkitMissingResourceError(
                    f"Cannot {operation}. Data set with external ID {data_sets!r} not found"
                )
            data_set_ids = (data_set_id,)
        elif isinstance(data_sets, list) and all(isinstance(item, str) for item in data_sets):
            data_set_ids_list = self.client.lookup.data_sets.id(external_id=data_sets, allow_empty=False)
            if len(data_set_ids_list) != len(data_sets):
                missing = set(data_sets) - set(
                    self.client.lookup.data_sets.external_id([id_ for id_ in data_set_ids_list if id_ is not None])
                )
                raise ToolkitMissingResourceError(
                    f"Cannot {operation}. Data sets with external IDs {sorted(missing)!r} not found"
                )
            data_set_ids = tuple(sorted(data_set_ids_list))

        return hierarchy_ids, data_set_ids

    @classmethod
    def create_filter(
        cls,
        hierarchy: str | list[str] | tuple[str, ...] | None = None,
        data_set_external_id: str | list[str] | tuple[str, ...] | None = None,
    ) -> ClassicFilter | None:
        """Creates a filter for the resource based on hierarchy and data set external ID."""
        asset_subtree_ids = cls._as_external_ids(hierarchy)
        data_set_ids = cls._as_external_ids(data_set_external_id)
        if asset_subtree_ids is None and data_set_ids is None:
            return None
        if asset_subtree_ids is None:
            return ClassicFilter(data_set_ids=data_set_ids)
        if data_set_ids is None:
            return ClassicFilter(asset_subtree_ids=asset_subtree_ids)
        return ClassicFilter(asset_subtree_ids=asset_subtree_ids, data_set_ids=data_set_ids)

    @staticmethod
    def _as_external_ids(items: str | list[str] | tuple[str, ...] | None) -> list[ExternalId | InternalId] | None:
        if isinstance(items, str):
            return [ExternalId(external_id=items)]
        if not items:
            return None
        return [ExternalId(external_id=item) for item in items]


class AssetAggregator(MetadataAggregator):
    _transformation_destination = ("assets", "asset_hierarchy")

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
        return self.client.tool.assets.count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.tool.assets.unique(("dataSetId",), filter=self.create_filter(hierarchy))
        return self._data_set_external_ids([bucket.value for bucket in results], coerce=False)


class EventAggregator(MetadataAggregator):
    _transformation_destination = ("events",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "events")

    @property
    def display_name(self) -> str:
        return "Events"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.tool.events.count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.tool.events.unique(("dataSetId",), filter=self.create_filter(hierarchy))
        return self._data_set_external_ids([bucket.value for bucket in results], coerce=False)


class FileAggregator(MetadataAggregator):
    _transformation_destination = ("files",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "files")

    @property
    def display_name(self) -> str:
        return "Files"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.tool.filemetadata.count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        # Files aggregate only returns a count, so distinct data set IDs come from the documents API.
        filter_: dict[str, Any] | None = None
        if hierarchy is not None:
            filter_ = {"inAssetSubtree": {"property": ["assetExternalIds"], "values": [hierarchy]}}
        results = self.client.tool.documents.unique(("sourceFile", "dataSetId"), filter=filter_, limit=1000)
        return self._data_set_external_ids([bucket.value for bucket in results], coerce=True)


class TimeSeriesAggregator(MetadataAggregator):
    _transformation_destination = ("timeseries",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "timeseries")

    @property
    def display_name(self) -> str:
        return "TimeSeries"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.tool.timeseries.count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.tool.timeseries.unique(("dataSetId",), filter=self.create_filter(hierarchy))
        return self._data_set_external_ids([bucket.value for bucket in results], coerce=True)


class SequenceAggregator(MetadataAggregator):
    _transformation_destination = ("sequences",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "sequences")

    @property
    def display_name(self) -> str:
        return "Sequences"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        return self.client.tool.sequences.count(filter=self.create_filter(hierarchy, data_set_external_id))

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        """Returns a list of data sets used by the resource."""
        results = self.client.tool.sequences.unique(("dataSetId",), filter=self.create_filter(hierarchy))
        return self._data_set_external_ids([bucket.value for bucket in results], coerce=True)


class RelationshipAggregator(AssetCentricAggregator):
    _transformation_destination = ("relationships",)

    @property
    def display_name(self) -> str:
        return "Relationships"

    def count(
        self, hierarchy: str | list[str] | None = None, data_set_external_id: str | list[str] | None = None
    ) -> int:
        if hierarchy is not None:
            raise NotImplementedError()
        dataset_id = self._to_dataset_id(data_set_external_id)
        results = relationship_aggregate_count(self.client, dataset_id)
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
        if hierarchy is not None:
            raise NotImplementedError()
        data_set_id = self._to_dataset_id(data_set_external_id)
        return label_aggregate_count(self.client, data_set_id)

    def used_data_sets(self, hierarchy: str | None = None) -> list[str]:
        raise NotImplementedError()
