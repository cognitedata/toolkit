from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import final

from cognite.client.data_classes.capabilities import Capability, LocationFiltersAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
    LocationFilterWriteList,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables


@final
class LocationFilterLoader(
    ResourceLoader[str, LocationFilterWrite, LocationFilter, LocationFilterWriteList, LocationFilterList]
):
    folder_name = "locations"
    filename_pattern = r"^.*LocationFilter$"
    resource_cls = LocationFilter
    resource_write_cls = LocationFilterWrite
    list_cls = LocationFilterList
    list_write_cls = LocationFilterWriteList
    kind = "LocationFilter"
    _doc_base_url = "https://api-docs.cogheim.net/redoc/#tag/"
    _doc_url = "Location-Filters/operation/createLocationFilter"

    @classmethod
    def get_required_capability(cls, items: LocationFilterWriteList) -> Capability | list[Capability]:
        if not items:
            return []
        return LocationFiltersAcl(
            actions=[LocationFiltersAcl.Action.Read, LocationFiltersAcl.Action.Write],
            scope=LocationFiltersAcl.Scope.All(),
            allow_unknown=True,
        )

    @classmethod
    def get_id(cls, item: LocationFilter | LocationFilterWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("LocationFilter must have external_id")
        return item.external_id

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> LocationFilterWrite | LocationFilterWriteList | None:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw_yaml, list):
            return self.list_write_cls.load(raw_yaml)
        elif isinstance(raw_yaml, dict):
            return self.resource_write_cls.load(raw_yaml)
        return None

    def create(self, items: LocationFilterWrite | LocationFilterWriteList) -> LocationFilterList:
        if isinstance(items, LocationFilterWrite):
            items = LocationFilterWriteList([items])

        created = []
        for item in items:
            created.append(self.client.locations.filters.create(item))
        return LocationFilterList(created)

    def retrieve(self, external_ids: SequenceNotStr[str]) -> LocationFilterList:
        return LocationFilterList(
            [loc for loc in self.client.locations.filters.list() if loc.external_id in external_ids]
        )

    def update(self, items: LocationFilterWrite | LocationFilterWriteList) -> LocationFilterList:
        if isinstance(items, LocationFilterWrite):
            items = LocationFilterWriteList([items])

        updated = []
        ids = {item.external_id: item.id for item in self.retrieve([item.external_id for item in items])}
        for update in items:
            updated.append(self.client.locations.filters.update(ids[update.external_id], update))
        return LocationFilterList(updated)

    def delete(self, external_ids: SequenceNotStr[str]) -> int:
        count = 0
        for id in [loc.id for loc in self.retrieve(external_ids)]:
            self.client.locations.filters.delete(id)
            count += 1
        return count

    def iterate(self) -> Iterable[LocationFilter]:
        return iter(self.client.locations.filters)
