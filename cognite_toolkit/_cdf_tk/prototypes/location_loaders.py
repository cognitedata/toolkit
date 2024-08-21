from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.locations import (
    LocationFilter,
    LocationFilterAcl,
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
        return LocationFilterAcl(
            actions=[LocationFilterAcl.Action.Read, LocationFilterAcl.Action.Write],
            scope=LocationFilterAcl.Scope.All(),
            allow_unknown=True,
        )

    @classmethod
    def get_id(self, item: LocationFilter | LocationFilterWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("LocationFilter must have external_id")
        return item.external_id

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> LocationFilterWrite | LocationFilterWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if isinstance(resources, dict):
            return LocationFilterWrite.load(resources)
        else:
            return LocationFilterWriteList.load(resources)

    def create(self, items: LocationFilterWriteList) -> LocationFilterList:
        return self.client.locations.location_filters.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> LocationFilterList:
        return LocationFilterList(
            [loc for loc in self.client.locations.location_filters.list() if loc.external_id in ids]
        )

    def update(self, items: LocationFilterWriteList) -> LocationFilterList:
        return self.client.locations.location_filters.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        return 0

    def iterate(self) -> Iterable[LocationFilter]:
        return iter(self.client.locations.location_filters)
