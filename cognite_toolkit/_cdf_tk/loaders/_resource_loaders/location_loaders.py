from __future__ import annotations

from collections.abc import Hashable, Iterable
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
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, in_dict, load_yaml_inject_variables

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
            DataModelLoader,
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

    @classmethod
    def get_required_capability(
        cls, items: LocationFilterWriteList | None, read_only: bool
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> LocationFilterWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

        raw_list = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]
        for raw in raw_list:
            if "parentExternalId" in raw:
                parent_external_id = raw.pop("parentExternalId")
                raw["parentId"] = ToolGlobals.verify_locationfilter(
                    parent_external_id, skip_validation, action="replace parentExternalId with parentExternalId"
                )

            if "assetCentric" not in raw:
                continue
            asset_centric = raw["assetCentric"]
            if "dataSetExternalIds" in asset_centric:
                data_set_external_ids = asset_centric.pop("dataSetExternalIds")
                asset_centric["dataSetIds"] = [
                    ToolGlobals.verify_dataset(
                        data_set_external_id,
                        skip_validation,
                        action="replace dataSetExternalIds with dataSetIds in location filter",
                    )
                    for data_set_external_id in data_set_external_ids
                ]
            for subfilter_name in self.subfilter_names:
                subfilter = asset_centric.get(subfilter_name, {})
                if "dataSetExternalIds" in subfilter:
                    data_set_external_ids = asset_centric[subfilter_name].pop("dataSetExternalIds")
                    asset_centric[subfilter_name]["dataSetIds"] = [
                        ToolGlobals.verify_dataset(
                            data_set_external_id,
                            skip_validation,
                            action="replace dataSetExternalIds with dataSetIds in location filter",
                        )
                        for data_set_external_id in data_set_external_ids
                    ]
        return LocationFilterWriteList._load(raw_list)

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

    def iterate(self) -> Iterable[LocationFilter]:
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

    def _are_equal(
        self, local: LocationFilterWrite, cdf_resource: LocationFilter, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()

        if "assetCentric" in local_dumped and "assetCentric" in cdf_dumped:
            local_centric = local_dumped["assetCentric"]
            cdf_centric = cdf_dumped["assetCentric"]
            if (
                "dataSetIds" in local_centric
                and "dataSetIds" in cdf_centric
                and all(data_set_id == -1 for data_set_id in local_centric["dataSetIds"])
            ):
                # Dry run
                local_centric["dataSetIds"] = cdf_centric["dataSetIds"]
            for subfilter_name in self.subfilter_names:
                if subfilter_name in local_centric and subfilter_name in cdf_centric:
                    local_subfilter = local_centric[subfilter_name]
                    cdf_subfilter = cdf_centric[subfilter_name]
                    if (
                        "dataSetIds" in local_subfilter
                        and "dataSetIds" in cdf_subfilter
                        and all(data_set_id == -1 for data_set_id in local_subfilter["dataSetIds"])
                    ):
                        # Dry run
                        local_subfilter["dataSetIds"] = cdf_subfilter["dataSetIds"]

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)
