from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import ClassVar, Literal, TypeVar, overload

import questionary
from cognite.client.data_classes import (
    Asset,
    DataSet,
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
