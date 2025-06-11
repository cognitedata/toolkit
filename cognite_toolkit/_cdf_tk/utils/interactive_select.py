from abc import abstractmethod
from functools import lru_cache

import questionary
from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetList,
    DataSet,
    DataSetList,
    EventFilter,
    FileMetadataFilter,
    TimeSeriesFilter,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient


class AssetCentricInteractiveSelect:
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @lru_cache
    def aggregate_count(self, hierarchies: tuple[str, ...], data_sets: tuple[str, ...]) -> int:
        return self._aggregate_count(list(hierarchies), list(data_sets))

    @abstractmethod
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        raise NotImplementedError()

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
        """Interactively select hierarchies and data sets to dump."""
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
                f"\n{selected_str}\nSelect a hierarchy or data set to dump",
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
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.assets.aggregate_count(
            filter=AssetFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )


class FileMetadataInteractiveSelect(AssetCentricInteractiveSelect):
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        result = self.client.files.aggregate(
            filter=FileMetadataFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )
        return result[0].count if result else 0


class TimeSeriesInteractiveSelect(AssetCentricInteractiveSelect):
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.time_series.aggregate_count(
            filter=TimeSeriesFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )


class EventInteractiveSelect(AssetCentricInteractiveSelect):
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.events.aggregate_count(
            filter=EventFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )
