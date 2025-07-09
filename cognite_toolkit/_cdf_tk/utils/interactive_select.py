from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import ClassVar, Literal

import questionary
from cognite.client.data_classes import (
    Asset,
    AssetList,
    DataSet,
    DataSetList,
    filters,
)
from cognite.client.data_classes.data_modeling import NodeList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import Canvas
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError

from .aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    TimeSeriesAggregator,
)


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

    @lru_cache(maxsize=1)
    def _get_available_data_sets(self) -> DataSetList:
        return self.client.data_sets.list(limit=-1)

    @lru_cache(maxsize=1)
    def _get_available_hierarchies(self) -> AssetList:
        return self.client.assets.list(root=True, limit=-1)

    def _create_choice(self, item: Asset | DataSet) -> tuple[questionary.Choice, int]:
        """Create a questionary choice for the given item."""

        if isinstance(item, DataSet):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for DataSet {item.id}")
            item_count = self.aggregate_count(tuple(), (item.external_id,))
        elif isinstance(item, Asset):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for Asset {item.id}")
            item_count = self.aggregate_count((item.external_id,), tuple())
        else:
            raise TypeError(f"Unsupported item type: {type(item)}")

        return questionary.Choice(
            title=f"{item.name} ({item.external_id}) [{item_count:,}]"
            if item.name != item.external_id
            else f"({item.external_id}) [{item_count:,}]",
            value=item.external_id,
        ), item_count

    def interactive_select_hierarchy_datasets(self) -> tuple[list[str], list[str]]:
        """Interactively select hierarchies and data sets."""
        hierarchies: set[str] = set()
        data_sets: set[str] = set()
        while True:
            selected: list[str] = []
            if hierarchies:
                selected.append(f"Selected hierarchies: {sorted(hierarchies)}")
            else:
                selected.append("No hierarchy selected.")
            if data_sets:
                selected.append(f"Selected data sets: {sorted(data_sets)}")
            else:
                selected.append("No data set selected.")
            selected_str = "\n".join(selected)
            what = questionary.select(
                f"\n{selected_str}\nSelect a hierarchy or data set to {self.operation}",
                choices=["Hierarchy", "Data Set", "Done", "Abort"],
            ).ask()

            if what == "Done":
                break
            elif what == "Abort":
                return [], []
            elif what == "Hierarchy":
                options = [asset for asset in self._get_available_hierarchies() if asset.external_id not in hierarchies]
                selected_hierarchy = self._select(what, options)
                if selected_hierarchy:
                    hierarchies.update(selected_hierarchy)
                else:
                    print("No hierarchy selected.")
            elif what == "Data Set":
                available_data_sets = [
                    data_set for data_set in self._get_available_data_sets() if data_set.external_id not in data_sets
                ]
                selected_data_set = self._select(what, available_data_sets)
                if selected_data_set:
                    data_sets.update(selected_data_set)
                else:
                    print("No data set selected.")
        return list(hierarchies), list(data_sets)

    def _select(self, what: str, options: list[Asset] | list[DataSet]) -> str | None:
        return questionary.checkbox(
            f"Select a {what} listed as 'name (external_id) [count]'",
            choices=[
                choice
                for choice, count in (
                    # MyPy does not seem to understand that item is Asset | DataSet
                    self._create_choice(item)  # type: ignore[arg-type]
                    for item in sorted(options, key=lambda x: x.name or x.external_id)
                )
                if count > 0
            ],
        ).ask()


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
