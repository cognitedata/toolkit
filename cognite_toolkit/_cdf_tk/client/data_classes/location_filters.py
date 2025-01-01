from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

# We need to import Sequence from typing and not collections.abc for
# cognite_toolkit._parameters.read_parameter_from_init_type_hints to work on Python 3.9
# is necessary to avoid Ruff changing the import
from typing import Any, Literal, Sequence  # noqa

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling.ids import DataModelId, ViewId
from typing_extensions import Self


@dataclass
class LocationFilterScene(CogniteObject):
    external_id: str
    space: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            space=resource["space"],
        )


@dataclass
class LocationFilterDataModel(CogniteObject):
    external_id: str
    space: str
    version: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            space=resource["space"],
            version=resource["version"],
        )


@dataclass(frozen=True)
class LocationFilterView(ViewId):
    represents_entity: Literal["MAINTENANCE_ORDER", "OPERATION", "NOTIFICATION", "ASSET"] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            space=resource["space"],
            version=resource["version"],
            represents_entity=resource.get("representsEntity"),
        )

    def dump(self, camel_case: bool = True, include_type: bool = False) -> dict[str, str]:
        if include_type:
            raise ValueError(f"{type(self).__name__} cannot be dumped with type")
        output = super().dump(camel_case, False)
        if self.represents_entity:
            output["representsEntity"] = self.represents_entity
        return output


@dataclass
class AssetCentricSubFilter(CogniteObject):
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[dict[Literal["externalId", "id"], int | str]] | None = None
    external_id_prefix: str | None = None


@dataclass
class AssetCentricFilter(CogniteObject):
    assets: AssetCentricSubFilter | None = None
    events: AssetCentricSubFilter | None = None
    files: AssetCentricSubFilter | None = None
    timeseries: AssetCentricSubFilter | None = None
    sequences: AssetCentricSubFilter | None = None
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[dict[Literal["externalId", "id"], int | str]] | None = None
    external_id_prefix: str | None = None

    @classmethod
    def _load_subfilter(cls, resource: dict[str, Any], key: str) -> AssetCentricSubFilter | None:
        if key in resource:
            return AssetCentricSubFilter.load(resource[key])
        return None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AssetCentricFilter:
        return cls(
            assets=cls._load_subfilter(resource, "assets"),
            events=cls._load_subfilter(resource, "events"),
            files=cls._load_subfilter(resource, "files"),
            timeseries=cls._load_subfilter(resource, "timeseries"),
            sequences=cls._load_subfilter(resource, "sequences"),
            data_set_ids=resource.get("dataSetIds"),
            asset_subtree_ids=resource.get("assetSubtreeIds"),
            external_id_prefix=resource.get("externalIdPrefix"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.assets:
            output["assets"] = self.assets.dump(camel_case)
        if self.events:
            output["events"] = self.events.dump(camel_case)
        if self.files:
            output["files"] = self.files.dump(camel_case)
        if self.timeseries:
            output["timeseries"] = self.timeseries.dump(camel_case)
        if self.sequences:
            output["sequences"] = self.sequences.dump(camel_case)
        return output


class LocationFilterCore(WriteableCogniteResource["LocationFilterWrite"], ABC):
    """
    LocationFilter contains information for a single LocationFilter.

    Args:
        external_id: The external ID provided by the client. Must be unique for the resource type.
        name: The name of the location filter
        parent_id: The ID of the parent location filter
        description: The description of the location filter
        data_models: The data models in the location filter
        instance_spaces: The list of spaces that instances are in
        scene: The scene config for the location filter
        asset_centric: The filter definition for asset centric resource types
        views: The view mappings for the location filter
    """

    def __init__(
        self,
        external_id: str,
        name: str,
        parent_id: int | None = None,
        description: str | None = None,
        data_models: list[DataModelId] | None = None,
        instance_spaces: list[str] | None = None,
        scene: LocationFilterScene | None = None,
        asset_centric: AssetCentricFilter | None = None,
        views: list[LocationFilterView] | None = None,
        data_modeling_type: Literal["HYBRID", "DATA_MODELING_ONLY"] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.data_models = data_models
        self.instance_spaces = instance_spaces
        self.scene = scene
        self.asset_centric = asset_centric
        self.views = views
        self.data_modeling_type = data_modeling_type

    def as_write(self) -> LocationFilterWrite:
        return LocationFilterWrite(
            external_id=self.external_id,
            name=self.name,
            parent_id=self.parent_id,
            description=self.description,
            data_models=self.data_models,
            instance_spaces=self.instance_spaces,
            scene=self.scene,
            asset_centric=self.asset_centric,
            views=self.views,
            data_modeling_type=self.data_modeling_type,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.data_models:
            output["dataModels" if camel_case else "data_models"] = [
                data_model.dump(camel_case, include_type=False) for data_model in self.data_models
            ]
        if self.scene:
            output["scene"] = self.scene.dump(camel_case)
        if self.asset_centric:
            output["assetCentric"] = self.asset_centric.dump(camel_case)
        if self.views:
            output["views"] = [view.dump(camel_case) for view in self.views]
        return output


class LocationFilterWrite(LocationFilterCore):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        scene = LocationFilterScene.load(resource.get("scene", {})) if resource.get("scene") else None
        data_models = (
            [DataModelId.load(item) for item in resource.get("dataModels", {})] if resource.get("dataModels") else None
        )
        asset_centric = (
            AssetCentricFilter.load(resource.get("assetCentric", {})) if resource.get("assetCentric") else None
        )
        views = [LocationFilterView._load(view) for view in resource["views"]] if "views" in resource else None
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            parent_id=resource.get("parentId"),
            description=resource.get("description"),
            data_models=data_models,
            instance_spaces=resource.get("instanceSpaces"),
            scene=scene,
            asset_centric=asset_centric,
            views=views,
            data_modeling_type=resource.get("dataModelingType"),
        )


class LocationFilter(LocationFilterCore):
    """
    LocationFilter contains information for a single LocationFilter

    Args:
        id: The ID of the location filter
        external_id: The external ID provided by the client. Must be unique for the resource type.
        name: The name of the location filter
        parent_id: The ID of the parent location filter
        description: The description of the location filter
        data_models: The data models in the location filter
        instance_spaces: The list of spaces that instances are in
        scene: The scene config for the location filter
        asset_centric: The filter definition for asset centric resource types
        views: The view mappings for the location filter
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        created_time: int,
        updated_time: int,
        parent_id: int | None = None,
        description: str | None = None,
        data_models: list[DataModelId] | None = None,
        instance_spaces: list[str] | None = None,
        scene: LocationFilterScene | None = None,
        asset_centric: AssetCentricFilter | None = None,
        views: list[LocationFilterView] | None = None,
        data_modeling_type: Literal["HYBRID", "DATA_MODELING_ONLY"] | None = None,
    ) -> None:
        super().__init__(
            external_id,
            name,
            parent_id,
            description,
            data_models,
            instance_spaces,
            scene,
            asset_centric,
            views,
            data_modeling_type,
        )
        self.id = id
        self.created_time = created_time
        self.updated_time = updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            name=resource["name"],
            parent_id=resource.get("parentId"),
            description=resource.get("description"),
            data_models=[DataModelId.load(item) for item in resource["dataModels"]]
            if "dataModels" in resource
            else None,
            instance_spaces=resource.get("instanceSpaces"),
            scene=LocationFilterScene._load(resource["scene"]) if "scene" in resource else None,
            asset_centric=AssetCentricFilter._load(resource["assetCentric"]) if "assetCentric" in resource else None,
            views=[LocationFilterView._load(view) for view in resource["views"]] if "views" in resource else None,
            created_time=resource["createdTime"],
            updated_time=resource["lastUpdatedTime"],
            data_modeling_type=resource.get("dataModelingType"),
        )


class LocationFilterWriteList(CogniteResourceList):
    _RESOURCE = LocationFilterWrite


class LocationFilterList(WriteableCogniteResourceList[LocationFilterWrite, LocationFilter]):
    _RESOURCE = LocationFilter

    def as_write(self) -> LocationFilterWriteList:
        return LocationFilterWriteList([LocationFilter.as_write() for LocationFilter in self])
