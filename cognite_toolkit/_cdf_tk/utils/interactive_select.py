from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import ClassVar, Literal, TypeVar, get_args, overload

import questionary
from cognite.client.data_classes import (
    Asset,
    DataSet,
    filters,
)
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import NodeList, Space, SpaceList, View, ViewId
from cognite.client.exceptions import CogniteException
from questionary import Choice
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import Canvas
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitValueError

from . import humanize_collection
from .aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    TimeSeriesAggregator,
)
from .useful_types import AssetCentricDestinationType

T_Type = TypeVar("T_Type", bound=Asset | DataSet)


class AssetCentricInteractiveSelect(ABC):
    def __init__(self, client: ToolkitClient, operation: str) -> None:
        self.client = client
        self.operation = operation
        self._aggregator = self._get_aggregator(client)

    @abstractmethod
    def _get_aggregator(self, client: ToolkitClient) -> AssetCentricAggregator:
        raise NotImplementedError()

    @lru_cache
    def aggregate_count(self, hierarchies: tuple[str, ...], data_sets: tuple[str, ...]) -> int:
        return self._aggregate_count(list(hierarchies), list(data_sets))

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self._aggregator.count(hierarchies, data_sets)

    @lru_cache
    def _get_available_data_sets(self, hierarchy: str | None = None) -> list[DataSet]:
        if hierarchy is not None:
            datasets = self._aggregator.used_data_sets(hierarchy)
            return list(self.client.data_sets.retrieve_multiple(external_ids=datasets))
        else:
            return list(self.client.data_sets.list(limit=-1))

    @lru_cache
    def _get_available_hierarchies(self, data_set: str | None = None) -> list[Asset]:
        data_set_external_ids = [data_set] if data_set else None
        return list(self.client.assets.list(root=True, limit=-1, data_set_external_ids=data_set_external_ids))

    def _create_choice(self, item: Asset | DataSet) -> tuple[questionary.Choice, int]:
        """Create a questionary choice for the given item."""
        if item.external_id is None:
            item_count = -1  # No count available for DataSet/Assets without external_id
        elif isinstance(item, DataSet):
            item_count = self.aggregate_count(tuple(), (item.external_id,))
        elif isinstance(item, Asset):
            item_count = self.aggregate_count((item.external_id,), tuple())
        else:
            raise ToolkitValueError(f"Unexpected item type: {type(item)}. Expected Asset or DataSet.")

        return questionary.Choice(
            title=f"{item.name} ({item.external_id}) [{item_count:,}]"
            if item.name != item.external_id
            else f"({item.external_id}) [{item_count:,}]",
            value=item.external_id,
        ), item_count

    @overload
    def select_hierarchy(self, allow_empty: Literal[False] = False) -> str: ...

    @overload
    def select_hierarchy(self, allow_empty: Literal[True]) -> str | None: ...

    def select_hierarchy(self, allow_empty: Literal[False, True] = False) -> str | None:
        """Select a hierarchy interactively."""
        options = self._get_available_hierarchies()
        return self._select("hierarchy", options, allow_empty, "single")

    def select_hierarchies(self) -> list[str]:
        options = self._get_available_hierarchies()
        return self._select("hierarchies", options, False, "multiple")

    @overload
    def select_data_set(self, allow_empty: Literal[False] = False) -> str: ...

    @overload
    def select_data_set(self, allow_empty: Literal[True]) -> str | None: ...

    def select_data_set(self, allow_empty: Literal[False, True] = False) -> str | None:
        """Select a data set interactively."""
        options = self._get_available_data_sets()
        return self._select("data set", options, allow_empty, "single")

    def select_data_sets(self) -> list[str]:
        """Select multiple data sets interactively."""
        options = self._get_available_data_sets()
        return self._select("data sets", options, False, "multiple")

    def select_hierarchies_and_data_sets(self) -> tuple[list[str], list[str]]:
        what = questionary.select(
            f"Do you want to {self.operation} a hierarchy or a data set?", choices=["Hierarchy", "Data Set"]
        ).ask()
        if what is None:
            raise ToolkitValueError("No selection made. Aborting.")
        if what == "Hierarchy":
            hierarchy = self._select("hierarchy", self._get_available_hierarchies(), False, "single")
            data_sets = self._get_available_data_sets(hierarchy=hierarchy)
            if not data_sets:
                return [hierarchy], []
            selected_data_sets = self._select(f"data sets in hierarchy {hierarchy!r}", data_sets, True, "multiple")
            return [hierarchy], selected_data_sets or []
        elif what == "Data Set":
            data_set = self._select("data Set", self._get_available_data_sets(), False, "single")
            hierarchies = self._get_available_hierarchies(data_set=data_set)
            if not hierarchies:
                return [], [data_set]
            selected_hierarchies = self._select(f"hierarchies in data set {data_set!r} ", hierarchies, True, "multiple")
            return selected_hierarchies or [], [data_set]
        else:
            raise ToolkitValueError(f"Unexpected selection: {what}. Aborting.")

    @overload
    def _select(
        self,
        what: str,
        options: Sequence[T_Type],
        allow_empty: Literal[False],
        plurality: Literal["single"],
    ) -> str: ...

    @overload
    def _select(
        self,
        what: str,
        options: Sequence[T_Type],
        allow_empty: Literal[True],
        plurality: Literal["single"],
    ) -> str | None: ...

    @overload
    def _select(
        self,
        what: str,
        options: Sequence[T_Type],
        allow_empty: Literal[False],
        plurality: Literal["multiple"],
    ) -> list[str]: ...

    @overload
    def _select(
        self,
        what: str,
        options: Sequence[T_Type],
        allow_empty: Literal[True],
        plurality: Literal["multiple"],
    ) -> list[str] | None: ...

    def _select(
        self,
        what: str,
        options: Sequence[Asset | DataSet],
        allow_empty: Literal[True, False],
        plurality: Literal["single", "multiple"],
    ) -> str | list[str] | None:
        """Select a single item interactively."""
        if not options and not allow_empty:
            raise ToolkitValueError(f"No {what} available to select.")

        choices: list[questionary.Choice] = []
        none_sentinel = object()
        if allow_empty:
            choices.append(questionary.Choice(title=f"All {what}", value=none_sentinel))

        for choice, count in sorted(
            (self._create_choice(item) for item in options), key=lambda x: (-x[1], x[0].title)
        ):  # cont, choice.title
            if count > 0:
                choices.append(choice)
        if not choices and not allow_empty:
            raise ToolkitValueError(f"No {what} available with data to select.")

        message = f"Select a {what} to {self.operation} listed as 'name (external_id) [count]'"
        if plurality == "multiple":
            selected = questionary.checkbox(message, choices=choices).ask()
        else:
            selected = questionary.select(message, choices=choices).ask()
        if selected is None:
            raise ToolkitValueError(f"No {what} selected. Aborting.")
        elif selected is none_sentinel or (isinstance(selected, list) and none_sentinel in selected):
            return None
        return selected


class AssetInteractiveSelect(AssetCentricInteractiveSelect):
    def _get_aggregator(self, client: ToolkitClient) -> AssetCentricAggregator:
        return AssetAggregator(self.client)


class FileMetadataInteractiveSelect(AssetCentricInteractiveSelect):
    def _get_aggregator(self, client: ToolkitClient) -> AssetCentricAggregator:
        return FileAggregator(self.client)


class TimeSeriesInteractiveSelect(AssetCentricInteractiveSelect):
    def _get_aggregator(self, client: ToolkitClient) -> AssetCentricAggregator:
        return TimeSeriesAggregator(self.client)


class EventInteractiveSelect(AssetCentricInteractiveSelect):
    def _get_aggregator(self, client: ToolkitClient) -> AssetCentricAggregator:
        return EventAggregator(self.client)


class RawTableInteractiveSelect:
    def __init__(self, client: ToolkitClient, operation: str) -> None:
        self.client = client
        self.operation = operation

    def _available_databases(self) -> list[str]:
        databases = self.client.raw.databases.list(limit=-1)
        return [db.name for db in databases if db.name is not None]

    def _available_tables(self, database: str) -> list[RawTable]:
        tables = self.client.raw.tables.list(db_name=database, limit=-1)
        return [RawTable(database, table.name) for table in tables if table.name is not None]

    def select_tables(self, database: str | None = None) -> list[RawTable]:
        """Interactively select raw tables."""
        databases = self._available_databases()
        if not databases:
            raise ToolkitValueError("No raw databases available. Aborting.")
        if database and database not in databases:
            raise ToolkitValueError(
                f"Database '{database}' not found in available raw databases: {databases}. Aborting."
            )
        elif database:
            selected_database = database
        else:
            selected_database = questionary.select(
                f"Select a Raw Database to {self.operation}",
                choices=[questionary.Choice(title=db, value=db) for db in databases],
            ).ask()
        if selected_database is None:
            raise ToolkitValueError("No database selected. Aborting.")
        available_tables = self._available_tables(selected_database)
        if not available_tables:
            raise ToolkitValueError(f"No raw tables available in database '{selected_database}'. Aborting.")

        selected_tables = questionary.checkbox(
            f"Select Raw Tables in {selected_database} to {self.operation}",
            choices=[questionary.Choice(title=f"{table.table_name}", value=table) for table in available_tables],
        ).ask()

        if selected_tables is None:
            raise ToolkitValueError("No tables selected. Aborting.")
        return selected_tables


@dataclass
class CanvasFilter:
    visibility: Literal["public", "private"] | None = None
    created_by: Literal["user"] | None = None
    select_all: bool = False

    def as_dms_filter(self) -> filters.Filter:
        canvas_id = Canvas.get_source()
        leaf_filters: list[filters.Filter] = [
            filters.Not(filters.Equals(canvas_id.as_property_ref("isArchived"), True)),
            # When sourceCanvasId is not set, we get the newest version of the canvas
            filters.Not(filters.Exists(canvas_id.as_property_ref("sourceCanvasId"))),
        ]
        if self.visibility is not None:
            leaf_filters.append(filters.Equals(canvas_id.as_property_ref("visibility"), self.visibility))

        return filters.And(*leaf_filters)


class InteractiveCanvasSelect:
    opening_choices: ClassVar[list[questionary.Choice]] = [
        questionary.Choice(title="All public Canvases", value=CanvasFilter(visibility="public", select_all=True)),
        questionary.Choice(title="Selected public Canvases", value=CanvasFilter(visibility="public", select_all=False)),
        questionary.Choice(title="All by given user", value=CanvasFilter(created_by="user", select_all=True)),
        questionary.Choice(title="Selected by given user", value=CanvasFilter(created_by="user", select_all=False)),
        questionary.Choice(title="All Canvases", value=CanvasFilter(visibility=None, select_all=True)),
    ]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    def select_external_ids(self) -> list[str]:
        select_filter = self._select_filter()

        return self._select_external_ids(select_filter)

    @classmethod
    def _select_filter(cls) -> CanvasFilter:
        user_response = questionary.select(
            "Which Canvases do you want to select?",
            choices=cls.opening_choices,
        ).ask()
        if user_response is None:
            raise ToolkitValueError("No Canvas selection made. Aborting.")
        return user_response

    def _select_external_ids(self, select_filter: CanvasFilter) -> list[str]:
        available_canvases = self.client.canvas.list(filter=select_filter.as_dms_filter(), limit=-1)
        if select_filter.select_all and select_filter.created_by is None:
            return [canvas.external_id for canvas in available_canvases]
        users = self.client.iam.user_profiles.list(limit=-1)
        display_name_by_user_identifier = {user.user_identifier: user.display_name or "missing" for user in users}
        if select_filter.created_by == "user":
            canvas_by_user: dict[str, list[Canvas]] = defaultdict(list)
            for canvas in available_canvases:
                canvas_by_user[canvas.created_by].append(canvas)

            user_response = questionary.select(
                "Which user do you want to select Canvases for?",
                choices=[
                    questionary.Choice(
                        title=f"{user.display_name} ({user.user_identifier})",
                        value=canvas_by_user[user.user_identifier],
                    )
                    for user in users
                    if user.user_identifier in canvas_by_user
                ],
            ).ask()
            available_canvases = NodeList[Canvas](user_response)

        if select_filter.select_all:
            return [canvas.external_id for canvas in available_canvases]

        selected_canvases = questionary.checkbox(
            "Select Canvases",
            choices=[
                questionary.Choice(
                    title=f"{canvas.name} (Created by {display_name_by_user_identifier[canvas.created_by]!r}, last updated {canvas.updated_at!r})",
                    value=canvas.external_id,
                )
                for canvas in available_canvases
            ],
        ).ask()

        return selected_canvases or []


class AssetCentricDestinationSelect:
    valid_destinations: tuple[str, ...] = get_args(AssetCentricDestinationType)

    @classmethod
    def validate(cls, destination_type: str) -> AssetCentricDestinationType:
        if destination_type not in cls.valid_destinations:
            raise ToolkitValueError(
                f"Invalid destination type: {destination_type!r}. Must be one of {humanize_collection(cls.valid_destinations)}."
            )
        # We validated the destination type above
        return destination_type  # type: ignore[return-value]

    @classmethod
    def select(cls) -> AssetCentricDestinationType:
        """Interactively select a destination type."""
        destination_type = questionary.select(
            "Select a destination type",
            choices=[questionary.Choice(title=dest.capitalize(), value=dest) for dest in cls.valid_destinations],
        ).ask()
        if destination_type is None:
            raise ToolkitValueError("No destination type selected. Aborting.")
        # We only input valid destination types, so we can safely skip MyPy's type checking here
        return destination_type  # type: ignore[return-value]

    @classmethod
    def get(cls, destination_type: str | None = None) -> AssetCentricDestinationType:
        """Get the destination type, either from the input or interactively."""
        if destination_type is None:
            return cls.select()
        return cls.validate(destination_type)


class DataModelingSelect:
    """A utility class to select Data Modeling nodes interactively."""

    def __init__(self, client: ToolkitClient, operation: str, console: Console | None = None) -> None:
        self.client = client
        self.operation = operation
        self.console = console or Console()
        self._available_spaces: SpaceList | None = None

    def select_view(self, include_global: bool = False) -> View:
        selected_space = self.select_space(
            include_global, message=f"In which Spaces is the view you will use to select instances to {self.operation}?"
        )
        views = self.client.data_modeling.views.list(
            space=selected_space.space,
            include_inherited_properties=True,
            limit=-1,
            include_global=include_global,
        )
        if not views:
            raise ToolkitMissingResourceError(f"No views found in space {selected_space.space!r}.")

        selected_view = questionary.select(
            f"Which view do you want to use to select instances to {self.operation}?",
            [Choice(title=f"{view.external_id} (version={view.version})", value=view) for view in views],
        ).ask()
        if selected_view is None:
            raise ToolkitValueError("No view selected")
        if not isinstance(selected_view, View):
            raise ToolkitValueError(f"Selected view is not a valid View object: {selected_view!r}")
        return selected_view

    def select_space(self, include_global: bool, message: str | None = None) -> Space:
        message = message or f"Select the space to {self.operation}:"
        spaces = self._get_available_spaces(include_global)
        selected_space = questionary.select(
            message,
            [Choice(title=space.space, value=space) for space in sorted(spaces, key=lambda s: s.space)],
        ).ask()
        if selected_space is None:
            raise ToolkitValueError("No space selected")
        if not isinstance(selected_space, Space):
            raise ToolkitValueError(f"Selected space is not a valid Space object: {selected_space!r}")
        return selected_space

    def select_instance_type(self, view_used_for: Literal["node", "edge", "all"]) -> Literal["node", "edge"]:
        if view_used_for != "all":
            return view_used_for
        selected_instance_type = questionary.select(
            f"What type of instances do you want to {self.operation}?",
            choices=[
                Choice(title="Nodes", value="node"),
                Choice(title="Edges", value="edge"),
            ],
        ).ask()
        if selected_instance_type is None:
            raise ToolkitValueError("No instance type selected")
        if selected_instance_type not in ["node", "edge"]:
            raise ToolkitValueError(f"Selected instance type is not valid: {selected_instance_type!r}")
        return selected_instance_type

    def select_instance_spaces(
        self, selected_view: ViewId, instance_type: Literal["node", "edge"] = "node"
    ) -> list[str] | None:
        all_spaces = self._get_available_spaces(include_global=False)
        count_by_space = self._get_instance_count_by_space(all_spaces, selected_view, instance_type)

        if not count_by_space:
            raise ToolkitMissingResourceError(
                f"No instances found in any space for the view {selected_view!r} with instance type {instance_type!r}."
            )
        if len(count_by_space) == 1:
            selected_spaces = next(iter(count_by_space.keys()))
            self.console.print(f"Only one space with instances found: {selected_spaces!r}. Using this space.")
            return [selected_spaces]

        selected_spaces = questionary.select(
            f"In which Space(s) do you want to {self.operation} instances?",
            choices=[
                Choice(title=f"{space} ({count:,} instances)", value=space)
                for space, count in sorted(count_by_space.items(), key=lambda item: item[1], reverse=True)
            ],
            multiselect=True,
        ).ask()
        if selected_spaces is None or len(selected_spaces) == 0:
            return None
        if not isinstance(selected_spaces, list):
            raise ToolkitValueError(f"Selected space is not a valid list: {selected_spaces!r}")
        return selected_spaces

    def _get_instance_count_by_space(
        self, all_spaces: SpaceList, view_id: ViewId, instance_type: Literal["node", "edge"]
    ) -> dict[str, float]:
        count_by_space: dict[str, float] = {}
        try:
            read_limit = self.client.data_modeling.statistics.project().concurrent_read_limit
        except CogniteException:
            # Fetching a broad exception as the statistics endpoint is in pre-alpha and may suddenly no longer be
            # available.
            read_limit = 2

        with ThreadPoolExecutor(max_workers=read_limit // 2) as executor:
            results = executor.map(
                partial(self._instance_count_space, view_id=view_id, instance_type=instance_type),
                (space.space for space in all_spaces),
            )
            for space_name, count in results:
                if count > 0:
                    count_by_space[space_name] = count

        return count_by_space

    @lru_cache
    def _instance_count_space(
        self, space: str, view_id: ViewId, instance_type: Literal["node", "edge"]
    ) -> tuple[str, float]:
        """Get the count of instances in a specific space for a given view and instance type."""
        return space, self.client.data_modeling.instances.aggregate(
            view_id, Count("externalId"), instance_type=instance_type, space=space
        ).value or 0.0

    def _get_available_spaces(self, include_global: bool = False) -> SpaceList:
        if self._available_spaces is None:
            self._available_spaces = self.client.data_modeling.spaces.list(include_global=True, limit=-1)
        if include_global:
            return self._available_spaces
        return SpaceList([space for space in self._available_spaces if not space.is_global])
