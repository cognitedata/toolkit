from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.capabilities import AllScope, Capability, IDScope, UnknownAcl
from cognite.client.data_classes.data_modeling.ids import DataModelId, IdLike, ViewId
from typing_extensions import Self


@dataclass(frozen=True)
class LocationFilterScene(IdLike):
    external_id: str
    space: str


@dataclass(frozen=True)
class LocationFilterDataModel(DataModelId):
    external_id: str
    space: str
    version: str


@dataclass(frozen=True)
class LocationFilterView(ViewId):
    external_id: str
    space: str
    version: str | None = None
    represents_entity: Literal["MAINTENANCE_ORDER", "OPERATION", "NOTIFICATION", "ASSET"] | None = None


@dataclass(frozen=True)
class LocationFilterAssetCentricBaseFilter:
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[int] | None = None
    external_id_prefix: str | None = None


@dataclass(frozen=True)
class LocationFilterAssetCentric:
    assets: LocationFilterAssetCentricBaseFilter | None = None
    events: LocationFilterAssetCentricBaseFilter | None = None
    files: LocationFilterAssetCentricBaseFilter | None = None
    timeseries: LocationFilterAssetCentricBaseFilter | None = None
    sequences: LocationFilterAssetCentricBaseFilter | None = None
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[int] | None = None
    external_id_prefix: str | None = None


@dataclass
class LocationFilterAcl(UnknownAcl):
    _capability_name = "locationFiltersAcl"
    actions: Sequence[Action]
    scope: AllScope | IDScope
    allow_unknown: bool = field(default=False, compare=False, repr=False)

    class Action(Capability.Action):  # type: ignore [misc]
        Read = "READ"
        Write = "WRITE"

    class Scope:
        All = AllScope
        SpaceID = IDScope


class LocationFilterCore(WriteableCogniteResource["LocationFilterWrite"], ABC):
    """
    LocationFilter contains information for a single LocationFilter.

    Args:
        externalId: The external ID provided by the client. Must be unique for the resource type.
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
        asset_centric: dict[str, Any] | None = None,
        views: list[str] | None = None,
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
        )


class LocationFilterWrite(LocationFilterCore):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            parent_id=resource.get("parentId"),
            description=resource.get("description"),
            data_models=resource.get("dataModels"),
            instance_spaces=resource.get("instanceSpaces"),
            scene=resource.get("scene"),
            asset_centric=resource.get("asseCentric"),
            views=resource.get("views"),
        )


class LocationFilter(LocationFilterCore):
    """
    LocationFilter contains information for a single LocationFilter

    Args:
        id: The ID of the location filter
        externalId: The external ID provided by the client. Must be unique for the resource type.
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
        asset_centric: dict[str, Any] | None = None,
        views: list[str] | None = None,
    ) -> None:
        super().__init__(
            external_id, name, parent_id, description, data_models, instance_spaces, scene, asset_centric, views
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
            data_models=resource.get("dataModels"),
            instance_spaces=resource.get("instanceSpaces"),
            scene=resource.get("scene"),
            asset_centric=resource.get("assetCentric"),
            views=resource.get("views"),
            created_time=resource["createdTime"],
            updated_time=resource["lastUpdatedTime"],
        )


class LocationFilterWriteList(CogniteResourceList):
    _RESOURCE = LocationFilterWrite


class LocationFilterList(WriteableCogniteResourceList[LocationFilterWrite, LocationFilter]):
    _RESOURCE = LocationFilter

    def as_write(self) -> LocationFilterWriteList:
        return LocationFilterWriteList([LocationFilter.as_write() for LocationFilter in self])
