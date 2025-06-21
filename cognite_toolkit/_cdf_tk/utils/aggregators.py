from abc import ABC, abstractmethod
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import (
    label_aggregate_count,
    label_count,
    metadata_key_counts,
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
    def count(self) -> int:
        raise NotImplementedError

    def transformation_count(self) -> int:
        """Returns the number of transformations associated with the resource."""
        transformation_count = 0
        for destination in self._transformation_destination:
            for chunk in self.client.transformations(chunk_size=1000, destination_type=destination, limit=None):
                transformation_count += len(chunk)
        return transformation_count


class MetadataAggregator(AssetCentricAggregator, ABC):
    def __init__(
        self, client: ToolkitClient, resource_name: Literal["assets", "events", "files", "timeseries", "sequences"]
    ) -> None:
        super().__init__(client)
        self.resource_name = resource_name

    def metadata_key_count(self) -> int:
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

    def count(self) -> int:
        return self.client.assets.aggregate_count()


class EventAggregator(MetadataAggregator):
    _transformation_destination = ("events",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "events")

    @property
    def display_name(self) -> str:
        return "Events"

    def count(self) -> int:
        return self.client.events.aggregate_count()


class FileAggregator(LabelAggregator):
    _transformation_destination = ("files",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "files")

    @property
    def display_name(self) -> str:
        return "Files"

    def count(self) -> int:
        response = self.client.files.aggregate()
        if response:
            return response[0].count
        else:
            return 0


class TimeSeriesAggregator(MetadataAggregator):
    _transformation_destination = ("timeseries",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "timeseries")

    @property
    def display_name(self) -> str:
        return "TimeSeries"

    def count(self) -> int:
        return self.client.time_series.aggregate_count()


class SequenceAggregator(MetadataAggregator):
    _transformation_destination = ("sequences",)

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "sequences")

    @property
    def display_name(self) -> str:
        return "Sequences"

    def count(self) -> int:
        return self.client.sequences.aggregate_count()


class RelationshipAggregator(AssetCentricAggregator):
    _transformation_destination = ("relationships",)

    @property
    def display_name(self) -> str:
        return "Relationships"

    def count(self) -> int:
        results = relationship_aggregate_count(self.client)
        return sum(result.count for result in results)


class LabelCountAggregator(AssetCentricAggregator):
    _transformation_destination = ("labels",)

    @property
    def display_name(self) -> str:
        return "Labels"

    def count(self) -> int:
        return label_aggregate_count(self.client)
