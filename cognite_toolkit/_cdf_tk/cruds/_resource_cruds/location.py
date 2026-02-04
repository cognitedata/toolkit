from collections.abc import Hashable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, LocationFiltersAcl
from cognite.client.data_classes.data_modeling import DataModelId, ViewId
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterRequest,
    LocationFilterResponse,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import ResourceRetrievalError, ToolkitCycleError
from cognite_toolkit._cdf_tk.resource_classes import LocationYAML
from cognite_toolkit._cdf_tk.utils import in_dict, quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier

from .classic import AssetCRUD, SequenceCRUD
from .data_organization import DataSetsCRUD
from .datamodel import DataModelCRUD, SpaceCRUD, ViewCRUD
from .file import FileMetadataCRUD
from .timeseries import TimeSeriesCRUD


@final
class LocationFilterCRUD(ResourceCRUD[ExternalId, LocationFilterRequest, LocationFilterResponse]):
    folder_name = "locations"
    resource_cls = LocationFilterResponse
    resource_write_cls = LocationFilterRequest
    yaml_cls = LocationYAML
    dependencies = frozenset(
        {
            AssetCRUD,
            DataSetsCRUD,
            DataModelCRUD,
            SpaceCRUD,
            ViewCRUD,
            SequenceCRUD,
            FileMetadataCRUD,
            TimeSeriesCRUD,
        }
    )
    kind = "LocationFilter"
    _doc_base_url = "https://api-docs.cogheim.net/redoc/#tag/"
    _doc_url = "Location-Filters/operation/createLocationFilter"

    subfilter_names = ("assets", "events", "files", "timeseries", "sequences")

    @property
    def display_name(self) -> str:
        return "location filters"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[LocationFilterRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        # Todo: Specify space ID scopes:

        actions = (
            [LocationFiltersAcl.Action.Read]
            if read_only
            else [LocationFiltersAcl.Action.Read, LocationFiltersAcl.Action.Write]
        )

        return LocationFiltersAcl(
            actions=actions,
            scope=LocationFiltersAcl.Scope.All(),
            allow_unknown=True,
        )

    @classmethod
    def get_id(cls, item: LocationFilterRequest | LocationFilterResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("LocationFilter must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    def safe_read(self, filepath: Path | str) -> str:
        # The version is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="version")

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return id.external_id

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> LocationFilterRequest:
        if parent_external_id := resource.pop("parentExternalId", None):
            # This is a workaround: when the parentExternalId cannot be resolved because the parent
            # hasn't been created yet, we save it so that we can try again "later"
            try:
                resource["parentId"] = self.client.lookup.location_filters.id(parent_external_id, is_dry_run)
            except ResourceRetrievalError:
                resource["parentId"] = -1
            # Store the parent external ID for topological sorting and late look up.
            resource["parentExternalId"] = parent_external_id

        if "assetCentric" not in resource:
            return LocationFilterRequest.model_validate(resource)
        asset_centric = resource["assetCentric"]
        if data_set_external_ids := asset_centric.pop("dataSetExternalIds", None):
            asset_centric["dataSetIds"] = self.client.lookup.data_sets.id(
                data_set_external_ids, is_dry_run, allow_empty=True
            )
        for subfilter_name in self.subfilter_names:
            subfilter = asset_centric.get(subfilter_name, {})
            if data_set_external_ids := subfilter.pop("dataSetExternalIds", []):
                asset_centric[subfilter_name]["dataSetIds"] = self.client.lookup.data_sets.id(
                    data_set_external_ids,
                    is_dry_run,
                    allow_empty=True,
                )

        return LocationFilterRequest.model_validate(resource)

    def dump_resource(self, resource: LocationFilterResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if parent_id := dumped.pop("parentId", None):
            dumped["parentExternalId"] = self.client.lookup.location_filters.external_id(parent_id)
        if dumped.get("dataModelingType") == "HYBRID" and local is not None and "dataModelingType" not in local:
            # Default set on server side
            dumped.pop("dataModelingType")
        if "assetCentric" not in dumped:
            return dumped
        asset_centric = dumped["assetCentric"]
        if data_set_ids := asset_centric.pop("dataSetIds", None):
            asset_centric["dataSetExternalIds"] = self.client.lookup.data_sets.external_id(data_set_ids)
        for subfilter_name in self.subfilter_names:
            subfilter = asset_centric.get(subfilter_name, {})
            if data_set_ids := subfilter.pop("dataSetIds", []):
                asset_centric[subfilter_name]["dataSetExternalIds"] = self.client.lookup.data_sets.external_id(
                    data_set_ids
                )
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "assetCentric" or json_path == ("instanceSpaces",):
            return diff_list_hashable(local, cdf)
        elif json_path in [("dataModels",), ("views",), ("scene",)]:
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    @classmethod
    def topological_sort(cls, items: Sequence[LocationFilterRequest]) -> list[LocationFilterRequest]:
        """Sorts the location filters in topological order based on their parent-child relationships."""
        location_by_id: dict[str, LocationFilterRequest] = {item.external_id: item for item in items}
        dependencies: dict[str, set[str]] = {}
        for item_id, item in location_by_id.items():
            dependencies[item_id] = set()
            # If this item has a parent, add it as a dependency
            if item.parent_external_id and item.parent_external_id in location_by_id:
                dependencies[item_id].add(item.parent_external_id)
        try:
            return [
                location_by_id[item_id]
                for item_id in TopologicalSorter(dependencies).static_order()
                if item_id in location_by_id
            ]
        except CycleError as e:
            raise ToolkitCycleError(
                f"Cannot deploy location filters. Cycle detected {e.args} in the parent-child dependencies of the location filters.",
                *e.args[1:],
            ) from None

    def create(self, items: Sequence[LocationFilterRequest]) -> list[LocationFilterResponse]:
        created: list[LocationFilterResponse] = []
        # Note: the Location API does not support batch creation, so we need to do this one by one.
        # Furthermore, we could not do the parentExternalId->parentId lookup before the parent was created,
        # hence it may be deferred here.
        # Use topological sort to ensure parents are created before children
        for item in self.topological_sort(items):
            # These are set if lookup has been deferred
            if item.parent_external_id and item.parent_id == -1:
                item.parent_id = self.client.lookup.location_filters.id(item.parent_external_id)
            created.extend(self.client.tool.location_filter.create([item]))
        return created

    def retrieve(self, external_ids: SequenceNotStr[ExternalId]) -> list[LocationFilterResponse]:
        # Use flat=True to get all locations in a flat list
        all_locations = self.client.tool.location_filter.list(flat=True)
        external_id_set = {ext_id.external_id for ext_id in external_ids}
        return [loc for loc in all_locations if loc.external_id in external_id_set]

    def update(self, items: Sequence[LocationFilterRequest]) -> list[LocationFilterResponse]:
        all_locations = self.client.tool.location_filter.list(flat=True)
        ids = {loc.external_id: loc.id for loc in all_locations}
        # Set the id on each item before updating
        for item in items:
            item.id = ids[item.external_id]
        return self.client.tool.location_filter.update(items)

    def delete(self, external_ids: SequenceNotStr[ExternalId]) -> int:
        locations = self.retrieve(external_ids)
        if not locations:
            return 0
        ids = [InternalId(id=loc.id) for loc in locations]
        self.client.tool.location_filter.delete(ids)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[LocationFilterResponse]:
        for chunk in self.client.tool.location_filter.iterate(flat=True):
            yield from chunk

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "assetCentric" in item:
            asset_centric = item["assetCentric"]
            for data_set_external_id in asset_centric.get("dataSetExternalIds", []):
                yield DataSetsCRUD, data_set_external_id
            for asset in asset_centric.get("assetSubtreeIds", []):
                if "externalId" in asset:
                    yield AssetCRUD, ExternalId(external_id=asset["externalId"])
            for subfilter_name in cls.subfilter_names:
                subfilter = asset_centric.get(subfilter_name, {})
                for data_set_external_id in subfilter.get("dataSetExternalIds", []):
                    yield DataSetsCRUD, data_set_external_id
                for asset in subfilter.get("assetSubtreeIds", []):
                    if "externalId" in asset:
                        yield AssetCRUD, ExternalId(external_id=asset["externalId"])
        for view in item.get("views", []):
            if in_dict(["space", "externalId", "version"], view):
                yield ViewCRUD, ViewId(view["space"], view["externalId"], view["version"])
        for space in item.get("instanceSpaces", []):
            yield SpaceCRUD, space
        for data_model in item.get("dataModels", []):
            if in_dict(["space", "externalId", "version"], data_model):
                yield DataModelCRUD, DataModelId(data_model["space"], data_model["externalId"], data_model["version"])
