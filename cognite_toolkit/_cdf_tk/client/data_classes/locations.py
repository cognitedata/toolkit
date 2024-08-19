from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.capabilities import AllScope, Capability, IDScope
from typing_extensions import Self


class LocationFilterDataModel:
    pass


class LocationFilterScene:
    pass


@dataclass
class LocationFilterAcl(Capability):
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

    """

    def __init__(
        self,
        external_id: str,
        name: str,
        parent_id: int | None = None,
        description: str | None = None,
        datamodels: list[LocationFilterDataModel] | None = None,
        instance_spaces: list[str] | None = None,
        scene: LocationFilterScene | None = None,
        asset_centric: dict[str, Any] | None = None,
        views: list[str] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.datamodels = datamodels
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
            datamodels=self.datamodels,
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
            datamodels=resource.get("datamodels"),
            instance_spaces=resource.get("instance_spaces"),
            scene=resource.get("scene"),
            asset_centric=resource.get("asset_centric"),
            views=resource.get("views"),
        )


class LocationFilter(LocationFilterCore):
    """
    LocationFilter contains information for a single LocationFilter

    Args:

    """

    def __init__(
        self,
        external_id: str,
        name: str,
        created_time: int,
        updated_time: int,
        parent_id: int | None = None,
        description: str | None = None,
        datamodels: list[LocationFilterDataModel] | None = None,
        instance_spaces: list[str] | None = None,
        scene: LocationFilterScene | None = None,
        asset_centric: dict[str, Any] | None = None,
        views: list[str] | None = None,
    ) -> None:
        super().__init__(
            external_id, name, parent_id, description, datamodels, instance_spaces, scene, asset_centric, views
        )
        self.created_time = created_time
        self.updated_time = updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            parent_id=resource.get("parentId"),
            description=resource.get("description"),
            datamodels=resource.get("datamodels"),
            instance_spaces=resource.get("instance_spaces"),
            scene=resource.get("scene"),
            asset_centric=resource.get("asset_centric"),
            views=resource.get("views"),
            created_time=resource["createdTime"],
            updated_time=resource["updatedTime"],
        )


class LocationFilterWriteList(CogniteResourceList):
    _RESOURCE = LocationFilterWrite


class LocationFilterList(WriteableCogniteResourceList[LocationFilterWrite, LocationFilter]):
    _RESOURCE = LocationFilter

    def as_write(self) -> LocationFilterWriteList:
        return LocationFilterWriteList([LocationFilter.as_write() for LocationFilter in self])
