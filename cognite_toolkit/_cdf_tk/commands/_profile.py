import itertools
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from typing import Any, ClassVar, Literal, TypeVar

from cognite.client.data_classes import (
    AssetFilter,
    EventFilter,
    FileMetadataFilter,
    SequenceFilter,
    TimeSeriesFilter,
    Transformation,
)
from cognite.client.data_classes.assets import AssetProperty
from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.events import EventProperty
from cognite.client.data_classes.sequences import SequenceProperty
from cognite.client.data_classes.time_series import TimeSeriesProperty
from cognite.client.exceptions import CogniteAPIError, CogniteException, CogniteReadTimeout
from rich import box
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.cdf import (
    get_transformation_sources,
    label_aggregate_count,
    label_count,
    metadata_key_counts,
    raw_row_count,
    relationship_aggregate_count,
)
from cognite_toolkit._cdf_tk.utils.interactive_select import AssetInteractiveSelect
from cognite_toolkit._cdf_tk.utils.sql_parser import SQLParser, SQLTable

from ._base import ToolkitCommand

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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

    @abstractmethod
    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        raise NotImplementedError

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

    def count(self, hierarchy: str | None = None, data_set_external_id: str | None = None) -> int:
        return self.client.assets.aggregate_count(filter=self._create_hierarchy_filter(hierarchy, data_set_external_id))

    @classmethod
    def _create_hierarchy_filter(
        cls, hierarchy: str | None, data_set_external_id: str | None = None
    ) -> AssetFilter | None:
        if hierarchy is None:
            return None
        return AssetFilter(
            asset_subtree_ids=[{"externalId": hierarchy}] if hierarchy else None,
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
        filter_ = self._create_hierarchy_filter(hierarchy)
        results = self.client.documents.aggregate_unique_values(
            property=SourceFileProperty.data_set_id, filter=filter_.dump() if filter_ else None
        )
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


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
        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


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

        return self.client.lookup.data_sets.external_id([id_ for id_ in results.unique if isinstance(id_, int)])


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


class ProfileCommand(ToolkitCommand):
    class Columns:
        Resource = "Resource"
        Count = "Count"
        MetadataKeyCount = "Metadata Key Count"
        LabelCount = "Label Count"
        Transformation = "Transformations"

    columns = (
        Columns.Resource,
        Columns.Count,
        Columns.MetadataKeyCount,
        Columns.LabelCount,
        Columns.Transformation,
    )
    spinner_speed = 1.0

    @classmethod
    def asset_centric(
        cls,
        client: ToolkitClient,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        aggregators: list[AssetCentricAggregator] = [
            AssetAggregator(client),
            EventAggregator(client),
            FileAggregator(client),
            TimeSeriesAggregator(client),
            SequenceAggregator(client),
            RelationshipAggregator(client),
            LabelCountAggregator(client),
        ]
        results, api_calls = cls._create_initial_table(aggregators)
        with Live(cls.create_profile_table(results), refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_cell = {
                    executor.submit(api_calls[(index, col)]): (index, col)
                    for index in range(len(aggregators))
                    for col in cls.columns
                    if (index, col) in api_calls
                }
                for future in as_completed(future_to_cell):
                    index, col = future_to_cell[future]
                    results[index][col] = future.result()
                    live.update(cls.create_profile_table(results))
        return [{col: str(value) for col, value in row.items()} for row in results]

    @classmethod
    def _create_initial_table(
        cls, aggregators: list[AssetCentricAggregator]
    ) -> tuple[list[dict[str, str | Spinner]], dict[tuple[int, str], Callable[[], str]]]:
        rows: list[dict[str, str | Spinner]] = []
        api_calls: dict[tuple[int, str], Callable[[], str]] = {}
        for index, aggregator in enumerate(aggregators):
            row: dict[str, str | Spinner] = {
                cls.Columns.Resource: aggregator.display_name,
                cls.Columns.Count: Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed),
            }
            api_calls[(index, cls.Columns.Count)] = cls._call_api(aggregator.count)
            count: str | Spinner = "-"
            if isinstance(aggregator, MetadataAggregator):
                count = Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed)
                api_calls[(index, cls.Columns.MetadataKeyCount)] = cls._call_api(aggregator.metadata_key_count)
            row[cls.Columns.MetadataKeyCount] = count

            count = "-"
            if isinstance(aggregator, LabelAggregator):
                count = Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed)
                api_calls[(index, cls.Columns.LabelCount)] = cls._call_api(aggregator.label_count)
            row[cls.Columns.LabelCount] = count

            row[cls.Columns.Transformation] = Spinner(
                "arc", text="loading...", style="bold green", speed=cls.spinner_speed
            )
            api_calls[(index, cls.Columns.Transformation)] = cls._call_api(aggregator.transformation_count)

            rows.append(row)
        return rows, api_calls

    @classmethod
    def create_profile_table(cls, rows: list[dict[str, str | Spinner]]) -> Table:
        table = Table(
            title="Asset Centric Profile",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        for col in cls.columns:
            table.add_column(col)

        for row in rows:
            table.add_row(*row.values())
        return table

    @staticmethod
    def _call_api(call_fun: Callable[[], int]) -> Callable[[], str]:
        def styled_callable() -> str:
            try:
                value = call_fun()
            except CogniteException as e:
                return type(e).__name__
            else:
                return f"{value:,}"

        return styled_callable


@dataclass(frozen=True)
class ResourceLineageID:
    resource: str
    dataset: str | None = None
    raw_table: str | None = None


class WaitingAPICallClass:
    def __bool__(self) -> bool:
        return False


WaitingAPICall = WaitingAPICallClass()


@dataclass
class ResourceLineageProfile:
    index: int
    resource: str
    count: int | WaitingAPICallClass | None = WaitingAPICall
    dataset: str | WaitingAPICallClass | None = WaitingAPICall
    dataset_count: int | WaitingAPICallClass | None = WaitingAPICall
    transformation: str | WaitingAPICallClass | None = WaitingAPICall
    raw_table: str | WaitingAPICallClass | None = WaitingAPICall
    row_count: int | str | WaitingAPICallClass | None = WaitingAPICall
    column_count: int | str | WaitingAPICallClass | None = WaitingAPICall

    def copy(self) -> Self:
        return self.__class__(
            index=self.index,
            resource=self.resource,
            count=self.count,
            dataset=self.dataset,
            dataset_count=self.dataset_count,
            transformation=self.transformation,
            raw_table=self.raw_table,
            row_count=self.row_count,
            column_count=self.column_count,
        )

    def as_sort_tuple(self) -> tuple[int, int, int]:
        return (
            self.index,
            -self.dataset_count if isinstance(self.dataset_count, int) else 0,
            -self.row_count if isinstance(self.row_count, int) else 0,
        )

    def as_row(self) -> tuple[str | Spinner, ...]:
        return (
            self.resource,
            self.as_cell(self.count),
            self.as_cell(self.dataset),
            self.as_cell(self.dataset_count),
            self.as_cell(self.transformation),
            self.as_cell(self.raw_table),
            self.as_cell(self.row_count),
            self.as_cell(self.column_count),
        )

    @staticmethod
    def as_cell(value: str | int | WaitingAPICallClass | None) -> str | Spinner:
        """Convert a value to a cell representation."""
        if isinstance(value, WaitingAPICallClass):
            return Spinner(name="arc", text="loading...", style="bold green", speed=1.0)
        elif isinstance(value, int):
            return f"{value:,}"
        elif isinstance(value, str):
            return value
        elif value is None:
            return ""
        else:
            raise ToolkitValueError(f"Unsupported type for cell conversion: {type(value)}")


T = TypeVar("T")


class ProfileAssetCommand(ToolkitCommand):
    class Columns:
        Resource = "Resource"
        Count = "Count"
        DataSets = "DataSet"
        DataSetCount = "DataSet Count"
        Transformations = "Transformation"
        RawTable = "Raw Table"
        RowCount = "Rows"
        ColumnCount = "Columns"

    columns = (
        Columns.Resource,
        Columns.Count,
        Columns.DataSets,
        Columns.DataSetCount,
        Columns.Transformations,
        Columns.RawTable,
        Columns.RowCount,
        Columns.ColumnCount,
    )
    spinner_args: Mapping[str, Any] = dict(name="arc", text="loading...", style="bold green", speed=1.0)

    def assets(
        self,
        client: ToolkitClient,
        hierarchy: str | None = None,
        verbose: bool = False,
    ) -> None:
        if hierarchy is None:
            hierarchies, _ = AssetInteractiveSelect(client).interactive_select_hierarchy_datasets()
            if len(hierarchies) > 1:
                raise ToolkitValueError("Profiling multiple hierarchies is not supported.")
            hierarchy = hierarchies[0]
        retrieved = client.assets.retrieve(external_id=hierarchy)
        if retrieved is None:
            raise ToolkitValueError(f"Hierarchy not found: {hierarchy}")
        elif retrieved.root_id != retrieved.id:
            raise ToolkitValueError(f"The asset {hierarchy} is not a root asset. Please select a root asset.")
        aggregators: dict[str, MetadataAggregator] = {
            aggregator.display_name: aggregator
            for aggregator in [
                AssetAggregator(client),
                TimeSeriesAggregator(client),
                FileAggregator(client),
                EventAggregator(client),
                SequenceAggregator(client),
            ]
        }
        api_calls = self._setup_initial_api_calls(hierarchy, aggregators)
        table_content: dict[ResourceLineageID, ResourceLineageProfile] = {
            ResourceLineageID(resource=agg_id): ResourceLineageProfile(index, agg_id)
            for index, agg_id in enumerate(aggregators.keys())
        }
        with Live(self._draw_table(table_content.values(), hierarchy), refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=8) as executor:
                while True:
                    current_calls: dict[Future, tuple[str, ...]] = {
                        executor.submit(call): key for key, call in api_calls.items()
                    }
                    next_calls: dict[tuple[str, ...], Callable] = {}
                    for future in as_completed(current_calls):
                        location = current_calls[future]
                        result = future.result()
                        self._update_next_calls(result, location, hierarchy, next_calls, aggregators)
                        self._update_table_content(result, location, table_content)
                        live.update(self._draw_table(table_content.values(), hierarchy))
                    if not next_calls:
                        break
                    api_calls = next_calls

        return None

    @classmethod
    def _setup_initial_api_calls(
        cls,
        hierarchy: str,
        aggregators: dict[str, MetadataAggregator],
    ) -> dict[tuple[str, ...], Callable]:
        api_calls: dict[tuple[str, ...], Callable] = {}
        for agg_id, aggregator in aggregators.items():
            api_calls[(agg_id, cls.Columns.Count)] = cls._call_api(partial(aggregator.count, hierarchy=hierarchy))
        return api_calls

    @classmethod
    def _update_next_calls(
        cls,
        result: Any,
        location: tuple[str, ...],
        hierarchy: str,
        next_calls: dict[tuple[str, ...], Callable],
        aggregators: dict[str, MetadataAggregator],
    ) -> None:
        agg_id, col, *extra = location
        aggregator = aggregators[agg_id]
        if col == cls.Columns.Count and isinstance(result, int) and result > 0:
            next_calls[(agg_id, cls.Columns.DataSets)] = cls._call_api(
                partial(aggregator.used_data_sets, hierarchy=hierarchy)
            )
        elif col == cls.Columns.DataSets and isinstance(result, list) and result:
            for item in result:
                next_calls[(agg_id, cls.Columns.DataSetCount, item)] = cls._call_api(
                    partial(aggregator.count, data_set_external_id=item, hierarchy=hierarchy)
                )
        elif col == cls.Columns.DataSetCount and extra and isinstance(extra[0], str):
            dataset = extra[0]
            next_calls[(agg_id, cls.Columns.Transformations, dataset)] = cls._call_api(
                partial(aggregator.used_transformations, data_set_external_ids=[dataset])
            )
        elif (
            col == cls.Columns.Transformations
            and isinstance(result, list)
            and result
            and extra
            and isinstance(extra[0], str)
        ):
            dataset = extra[0]
            for transformation in result:
                sources = get_transformation_sources(transformation.query or "")
                for source in sources:
                    if isinstance(source, RawTable):
                        next_calls[(agg_id, cls.Columns.RowCount, dataset, str(source))] = ProfileRawCommand.row_count(
                            client=aggregator.client, raw_table=source
                        )
                        next_calls[(agg_id, cls.Columns.ColumnCount, dataset, str(source))] = (
                            ProfileRawCommand.column_count(client=aggregator.client, raw_table=source)
                        )

    @classmethod
    def _update_table_content(
        cls,
        result: Any,
        location: tuple[str, ...],
        table_content: dict[ResourceLineageID, ResourceLineageProfile],
    ) -> None:
        agg_id, col, *extra = location
        if col == cls.Columns.Count:
            value = table_content[ResourceLineageID(resource=agg_id)]
            if isinstance(result, int):
                value.count = result
            else:
                value.count = None
            if value.count is None or value.count == 0:
                value.dataset = None
                value.dataset_count = None
                value.transformation = None
                value.raw_table = None
                value.row_count = None
                value.column_count = None
        elif col == cls.Columns.DataSets:
            if isinstance(result, list) and result:
                current = table_content.pop(ResourceLineageID(resource=agg_id))
                for dataset in result:
                    copy_ = current.copy()
                    copy_.dataset = dataset
                    table_content[ResourceLineageID(resource=agg_id, dataset=dataset)] = copy_
            else:
                value = table_content[ResourceLineageID(resource=agg_id)]
                value.dataset = None
                value.dataset_count = None
                value.transformation = None
                value.raw_table = None
                value.row_count = None
                value.column_count = None
        elif col == cls.Columns.DataSetCount:
            dataset = extra[0] if extra else None
            value = table_content[ResourceLineageID(resource=agg_id, dataset=dataset)]
            if isinstance(result, int):
                value.dataset_count = result
            else:
                value.dataset_count = None
            if value.dataset_count is None or value.dataset_count == 0:
                value.transformation = None
                value.raw_table = None
                value.row_count = None
                value.column_count = None
        elif col == cls.Columns.Transformations:
            dataset = extra[0] if extra else None
            if isinstance(result, list) and result:
                current = table_content.pop(ResourceLineageID(resource=agg_id, dataset=dataset))
                for transformation in result:
                    copy_ = current.copy()
                    copy_.transformation = transformation.external_id or "<unknown>"
                    sources = get_transformation_sources(transformation.query or "")
                    if not sources:
                        table_content[ResourceLineageID(resource=agg_id, dataset=dataset)] = copy_
                        copy_.raw_table = None
                        copy_.row_count = None
                        copy_.column_count = None
                    else:
                        for source in sources:
                            copy_copy = copy_.copy()
                            if isinstance(source, str):
                                copy_copy.raw_table = f"_cdf.{source}"
                                copy_copy.row_count = None
                                copy_copy.column_count = None
                            else:
                                copy_copy.raw_table = str(source)
                            table_content[
                                ResourceLineageID(
                                    resource=agg_id,
                                    dataset=dataset,
                                    raw_table=str(source),
                                )
                            ] = copy_copy
            else:
                value = table_content[ResourceLineageID(resource=agg_id, dataset=dataset)]
                value.transformation = None
                value.raw_table = None
                value.row_count = None
                value.column_count = None
        elif col == cls.Columns.RowCount:
            dataset = extra[0] if extra else None
            raw_table = extra[1] if len(extra) > 1 else None
            value = table_content[ResourceLineageID(resource=agg_id, dataset=dataset, raw_table=raw_table)]
            if isinstance(result, int | str):
                value.row_count = result
            else:
                value.row_count = None
        elif col == cls.Columns.ColumnCount:
            dataset = extra[0] if extra else None
            raw_table = extra[1] if len(extra) > 1 else None
            value = table_content[ResourceLineageID(resource=agg_id, dataset=dataset, raw_table=raw_table)]
            if isinstance(result, int | str):
                value.column_count = result
            else:
                value.column_count = None

    @classmethod
    def _draw_table(
        cls,
        rows: Iterable[ResourceLineageProfile],
        hierarchy: str,
    ) -> Table:
        table = Table(
            title=f"Asset Profile for Hierarchy: {hierarchy!r}",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
            box=box.MINIMAL,
        )
        for col in cls.columns:
            table.add_column(col)
        content = list(rows)

        shown_by_index: dict[int, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
        seen_indexes = set()
        for group, rows in itertools.groupby(
            sorted(content, key=lambda x: x.as_sort_tuple()), key=lambda x: x.as_sort_tuple()
        ):
            index = group[0]
            shown = shown_by_index[index]
            for row in rows:
                this_row = row.as_row()
                draw_row = ["" if cell in shown[i] else cell for i, cell in enumerate(this_row)]
                table.add_row(*draw_row, style="bold" if index not in seen_indexes else "dim")
                for i, cell in enumerate(draw_row):
                    if isinstance(cell, str):
                        shown[i].add(cell)
                seen_indexes.add(index)
        return table

    @staticmethod
    def _call_api(call_fun: Callable[[], T]) -> Callable[[], T | str]:
        def styled_callable() -> T | str:
            try:
                return call_fun()
            except CogniteException as e:
                return type(e).__name__

        return styled_callable

    @staticmethod
    def _as_human_readable(value: int | str) -> str:
        """Convert a value to a human-readable string."""
        if isinstance(value, int):
            return f"{value:,}"
        elif isinstance(value, str):
            return value
        else:
            raise ToolkitValueError(f"Unsupported type for human-readable conversion: {type(value)}")


class ProfileRawCommand(ToolkitCommand):
    class Columns:
        RAW = "Raw"
        Rows = "Rows"
        Columns = "Columns"
        Transformation = "Transformation"
        Destination = "Destination"
        Operation = "Operation"
        UseAll = "Use All"

    columns = (
        Columns.RAW,
        Columns.Rows,
        Columns.Columns,
        Columns.Transformation,
        Columns.Destination,
        Columns.Operation,
        Columns.UseAll,
    )
    spinner_args: Mapping[str, Any] = dict(name="arc", text="loading...", style="bold green", speed=1.0)

    profile_row_limit = 10_000  # The number of rows to profile to get the number of columns.
    # The actual limit is 1 million, we typically run this against 30 tables and that high limit
    # will cause 504 errors.
    profile_timeout_seconds = 60 * 4  # Timeout for the profiling operation in seconds,
    # This is the same ase the run/query maximum timeout.

    @classmethod
    def raw(
        cls,
        client: ToolkitClient,
        destination_type: str,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        console = Console()
        with console.status("Preparing...", spinner="aesthetic", speed=0.4) as _:
            existing_tables = cls._get_existing_tables(client)
            transformations_by_raw_table = cls._get_transformations_by_raw_table(client, destination_type)

            table_content, api_calls, indices_by_raw_table = cls._setup_table_and_api_calls(
                client, transformations_by_raw_table, existing_tables
            )
        with Live(cls.draw_table(table_content, destination_type), refresh_per_second=4, console=console) as live:
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_cell = {
                    executor.submit(api_calls[(raw_id, col)]): (raw_id, col)
                    for raw_id in transformations_by_raw_table.keys()
                    for col in [cls.Columns.Rows, cls.Columns.Columns]
                    if (raw_id, col) in api_calls
                }
                for future in as_completed(future_to_cell):
                    raw_id, col = future_to_cell[future]
                    count = future.result()
                    index = indices_by_raw_table[raw_id]
                    table_content[index][col] = count
                    live.update(cls.draw_table(table_content, destination_type))
        return [{col: str(value) for col, value in row.items()} for row in table_content]

    @classmethod
    def _get_existing_tables(cls, client: ToolkitClient) -> set[RawTable]:
        existing_tables: set[RawTable] = set()
        databases = client.raw.databases.list(limit=-1)
        for database in databases:
            if database.name is None:
                continue
            tables = client.raw.tables.list(db_name=database.name, limit=-1)
            for table in tables:
                if table.name is None:
                    continue
                existing_tables.add(RawTable(db_name=database.name, table_name=table.name))
        return existing_tables

    @classmethod
    def _get_transformations_by_raw_table(
        cls, client: ToolkitClient, destination_type: str
    ) -> dict[RawTable, list[Transformation]]:
        transformations = client.transformations.list(destination_type=destination_type)
        if destination_type == "assets":
            transformations.extend(client.transformations.list(destination_type="asset_hierarchy"))
        transformations_by_raw_table: dict[RawTable, list[Transformation]] = defaultdict(list)
        for transformation in transformations:
            if transformation.query is None:
                # No query means no source table.
                continue
            sources = get_transformation_sources(transformation.query)
            for source in sources:
                if isinstance(source, RawTable):
                    transformations_by_raw_table[source].append(transformation)
        return transformations_by_raw_table

    @classmethod
    def _setup_table_and_api_calls(
        cls,
        client: ToolkitClient,
        transformations_by_raw_table: dict[RawTable, list[Transformation]],
        existing_tables: set[RawTable],
    ) -> tuple[list[dict[str, str | Spinner]], dict[tuple[RawTable, str], Callable[[], str]], dict[RawTable, int]]:
        rows: list[dict[str, str | Spinner]] = []
        api_calls: dict[tuple[RawTable, str], Callable[[], str]] = {}
        index_by_raw_id: dict[RawTable, int] = {}
        index = 0
        for raw_id, transformations in transformations_by_raw_table.items():
            is_existing = raw_id in existing_tables
            if is_existing:
                api_calls[(raw_id, cls.Columns.Rows)] = cls.row_count(client, raw_id)
                api_calls[(raw_id, cls.Columns.Columns)] = cls.column_count(client, raw_id)
            for no, transformation in enumerate(transformations):
                is_first = no == 0
                existing_str = " (missing)" if not is_existing else ""
                row: dict[str, str | Spinner] = {
                    cls.Columns.RAW: f"{raw_id.db_name}.{raw_id.table_name}{existing_str}" if is_first else "",
                    cls.Columns.Rows: Spinner(**cls.spinner_args) if is_first and is_existing else "",
                    cls.Columns.Columns: Spinner(**cls.spinner_args) if is_first and is_existing else "",
                    cls.Columns.Transformation: transformation.name or transformation.external_id or "Unknown",
                    cls.Columns.Destination: transformation.destination.type
                    if transformation.destination and transformation.destination.type
                    else "Unknown",
                    cls.Columns.Operation: transformation.conflict_mode or "Unknown",
                    cls.Columns.UseAll: str("where" in (transformation.query or "unknown").casefold()),
                }
                rows.append(row)
                if is_first:
                    index_by_raw_id[raw_id] = index
                index += 1
        return rows, api_calls, index_by_raw_id

    @classmethod
    def draw_table(cls, rows: list[dict[str, str | Spinner]], destination: str) -> Table:
        table = Table(
            title=f"RAW Profile destination: {destination}",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        for col in cls.columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*row.values())
        return table

    @classmethod
    def column_count(cls, client: ToolkitClient, raw_table: RawTable) -> Callable[[], str]:
        def api_call() -> str:
            try:
                # MyPy does not understand that ToolkitClient.raw.profile exists, it fails to account for the override
                # in the init of the ToolkitClient class.
                result = client.raw.profile(  # type: ignore[attr-defined]
                    raw_table, limit=cls.profile_row_limit, timeout_seconds=cls.profile_timeout_seconds
                )
            except CogniteAPIError as e1:
                return f"{type(e1).__name__}({e1.code})"
            except CogniteReadTimeout:
                return f"Read timeout {cls.profile_timeout_seconds} exceeded"
            except CogniteException as e3:
                return type(e3).__name__
            else:
                output = f"{result.column_count:,}"
                if not result.is_complete or result.row_count >= cls.profile_row_limit:
                    output = "≥" + output
                return output

        return api_call

    @classmethod
    def row_count(cls, client: ToolkitClient, raw_table: RawTable) -> Callable[[], str]:
        def api_call() -> str:
            try:
                count = raw_row_count(client, raw_table)
            except CogniteAPIError as e1:
                return f"{type(e1).__name__}({e1.code})"
            except CogniteException as e2:
                return type(e2).__name__
            else:
                return f"{count:,}"

        return api_call


class ProfileTransformationCommand(ToolkitCommand):
    class Columns:
        Transformation = "Transformation"
        Source = "Sources"
        DestinationColumns = "Destination Columns"
        Destination = "Destination"
        ConflictMode = "Conflict Mode"
        IsPaused = "Is Paused"

    columns = (
        Columns.Transformation,
        Columns.Source,
        Columns.DestinationColumns,
        Columns.Destination,
        Columns.ConflictMode,
        Columns.IsPaused,
    )

    @classmethod
    def transformation(
        cls,
        client: ToolkitClient,
        destination_type: str,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        console = Console()
        content: list[dict[str, str]] = []
        with console.status("Loading transformations...", spinner="aesthetic", speed=0.4) as _:
            iterable: Iterable[Transformation] = client.transformations.list(destination_type=destination_type)
            if destination_type == "assets":
                iterable = itertools.chain(iterable, client.transformations(destination_type="asset_hierarchy"))
            for transformation in iterable:
                sources: list[SQLTable] = []
                destination_columns: list[str] = []
                if transformation.query:
                    parser = SQLParser(transformation.query, operation="Profile transformations")
                    sources = parser.sources
                    destination_columns = parser.destination_columns
                row: dict[str, str] = {
                    cls.Columns.Transformation: transformation.name or transformation.external_id or "Unknown",
                    cls.Columns.Source: ", ".join(map(str, sources)),
                    cls.Columns.DestinationColumns: ", ".join(destination_columns) or "None",
                    cls.Columns.Destination: transformation.destination.type or "Unknown"
                    if transformation.destination
                    else "Unknown",
                    cls.Columns.ConflictMode: transformation.conflict_mode or "Unknown",
                    cls.Columns.IsPaused: str(transformation.schedule.is_paused)
                    if transformation.schedule
                    else "No schedule",
                }
                content.append(row)

        table = cls.draw_table(content, destination_type)
        console.print(table)
        return content

    @classmethod
    def draw_table(
        cls,
        rows: list[dict[str, str]],
        destination: str,
    ) -> Table:
        table = Table(
            title=f"Transformation Profile destination: {destination}",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        for col in cls.columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*row.values())
        return table
