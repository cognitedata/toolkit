from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, LocationFiltersAcl
from cognite.client.data_classes.data_modeling import DataModelId, ViewId
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.location_filters import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
    LocationFilterWriteList,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import in_dict, quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier

from .classic_loaders import AssetLoader, SequenceLoader
from .data_organization_loaders import DataSetsLoader
from .datamodel_loaders import DataModelLoader, SpaceLoader, ViewLoader
from .file_loader import FileMetadataLoader
from .timeseries_loaders import TimeSeriesLoader


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
    dependencies = frozenset(
        {
            AssetLoader,
            DataSetsLoader,
            DataModelLoader,
            SpaceLoader,
            ViewLoader,
            SequenceLoader,
            FileMetadataLoader,
            TimeSeriesLoader,
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
        cls, items: Sequence[LocationFilterWrite] | None, read_only: bool
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
    def get_id(cls, item: LocationFilter | LocationFilterWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("LocationFilter must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    def safe_read(self, filepath: Path | str) -> str:
        # The version is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath), key="version")

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> LocationFilterWrite:
        if parent_external_id := resource.pop("parentExternalId", None):
            resource["parentId"] = self.client.lookup.location_filters.id(parent_external_id, is_dry_run)
        if "assetCentric" not in resource:
            return LocationFilterWrite._load(resource)
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

        return LocationFilterWrite._load(resource)

    def dump_resource(self, resource: LocationFilter, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if parent_id := dumped.pop("parentId", None):
            dumped["parentExternalId"] = self.client.lookup.location_filters.external_id(parent_id)
        if "dataModelingType" in dumped and "dataModelingType" not in local:
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

    def create(self, items: LocationFilterWrite | LocationFilterWriteList) -> LocationFilterList:
        if isinstance(items, LocationFilterWrite):
            items = LocationFilterWriteList([items])

        created = []
        for item in items:
            created.append(self.client.location_filters.create(item))
        return LocationFilterList(created)

    def retrieve(self, external_ids: SequenceNotStr[str]) -> LocationFilterList:
        return LocationFilterList(
            [loc for loc in self.client.location_filters.list() if loc.external_id in external_ids]
        )

    def update(self, items: LocationFilterWrite | LocationFilterWriteList) -> LocationFilterList:
        if isinstance(items, LocationFilterWrite):
            items = LocationFilterWriteList([items])

        updated = []
        ids = {item.external_id: item.id for item in self.retrieve([item.external_id for item in items])}
        for update in items:
            updated.append(self.client.location_filters.update(ids[update.external_id], update))
        return LocationFilterList(updated)

    def delete(self, external_ids: SequenceNotStr[str]) -> int:
        count = 0
        for id in [loc.id for loc in self.retrieve(external_ids)]:
            self.client.location_filters.delete(id)
            count += 1
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[LocationFilter]:
        return iter(self.client.location_filters)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(
            ParameterSpec(
                ("parentExternalId",),
                frozenset({"str"}),
                is_required=False,
                _is_nullable=True,
            )
        )
        spec.discard(
            ParameterSpec(
                ("parentId",),
                frozenset({"int"}),
                is_required=False,
                _is_nullable=True,
            )
        )
        spec.add(
            ParameterSpec(
                (
                    "assetCentric",
                    "dataSetExternalIds",
                ),
                frozenset({"list"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.add(
            ParameterSpec(
                (
                    "assetCentric",
                    "dataSetExternalIds",
                    ANY_INT,
                ),
                frozenset({"str"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.discard(
            ParameterSpec(
                (
                    "assetCentric",
                    "dataSetIds",
                ),
                frozenset({"list"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.discard(
            ParameterSpec(
                (
                    "assetCentric",
                    "dataSetIds",
                    ANY_INT,
                ),
                frozenset({"int"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.discard(
            ParameterSpec(
                ("assetCentric", "assetSubtreeIds", ANY_INT, "externalId"), frozenset({"str", "int"}), False, False
            )
        )
        spec.add(
            ParameterSpec(("assetCentric", "assetSubtreeIds", ANY_INT, "externalId"), frozenset({"str"}), False, False)
        )
        spec.add(ParameterSpec(("assetCentric", "assetSubtreeIds", ANY_INT, "id"), frozenset({"int"}), False, False))
        for subfilter_name in cls.subfilter_names:
            spec.discard(
                ParameterSpec(
                    ("assetCentric", subfilter_name, "assetSubtreeIds", ANY_INT, "externalId"),
                    frozenset({"str", "int"}),
                    False,
                    False,
                )
            )
            spec.add(
                ParameterSpec(
                    ("assetCentric", subfilter_name, "assetSubtreeIds", ANY_INT, "externalId"),
                    frozenset({"str"}),
                    False,
                    False,
                )
            )
            spec.add(
                ParameterSpec(
                    ("assetCentric", subfilter_name, "assetSubtreeIds", ANY_INT, "id"), frozenset({"int"}), False, False
                )
            )
            spec.add(
                ParameterSpec(("assetCentric", subfilter_name, "dataSetExternalIds"), frozenset({"list"}), False, False)
            )
            spec.add(
                ParameterSpec(
                    ("assetCentric", subfilter_name, "dataSetExternalIds", ANY_INT), frozenset({"str"}), False, False
                )
            )
            spec.discard(
                ParameterSpec(("assetCentric", subfilter_name, "dataSetIds"), frozenset({"list"}), False, False)
            )
            spec.discard(
                ParameterSpec(("assetCentric", subfilter_name, "dataSetIds", ANY_INT), frozenset({"int"}), False, False)
            )

        spec.add(ParameterSpec(("dataModels", ANY_INT, "type"), frozenset({"str"}), False, False))
        spec.add(ParameterSpec(("views", ANY_INT, "type"), frozenset({"str"}), False, False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "assetCentric" in item:
            asset_centric = item["assetCentric"]
            for data_set_external_id in asset_centric.get("dataSetExternalIds", []):
                yield DataSetsLoader, data_set_external_id
            for asset in asset_centric.get("assetSubtreeIds", []):
                if "externalId" in asset:
                    yield AssetLoader, asset["externalId"]
            for subfilter_name in cls.subfilter_names:
                subfilter = asset_centric.get(subfilter_name, {})
                for data_set_external_id in subfilter.get("dataSetExternalIds", []):
                    yield DataSetsLoader, data_set_external_id
                for asset in subfilter.get("assetSubtreeIds", []):
                    if "externalId" in asset:
                        yield AssetLoader, asset["externalId"]
        for view in item.get("views", []):
            if in_dict(["space", "externalId", "version"], view):
                yield ViewLoader, ViewId(view["space"], view["externalId"], view["version"])
        for space in item.get("instanceSpaces", []):
            yield SpaceLoader, space
        for data_model in item.get("dataModels", []):
            if in_dict(["space", "externalId", "version"], data_model):
                yield DataModelLoader, DataModelId(data_model["space"], data_model["externalId"], data_model["version"])
