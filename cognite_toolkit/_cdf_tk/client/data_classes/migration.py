from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedNode,
)


@dataclass(frozen=True)
class AssetCentricId(CogniteObject):
    resource_type: Literal["asset", "event", "file", "sequence", "timeseries"]
    id_: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AssetCentricId:
        """Load an AssetCentricId from a dictionary."""
        return cls(
            resource_type=resource["resourceType"],
            id_=resource["id"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the AssetCentricId to a dictionary."""
        return {
            "resourceType" if camel_case else "resource_type": self.resource_type,
            "id" if camel_case else "id_": self.id_,
        }

    def __str__(self) -> str:
        """Return a string representation of the AssetCentricId."""
        return f"{self.resource_type}(id={self.id_})"


class _MappingProperties:
    resource_type = PropertyOptions("resourceType")
    id_ = PropertyOptions("id")
    data_set_id = PropertyOptions("dataSetId")
    classic_external_id = PropertyOptions("classicExternalId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cognite_migration", "Mapping", "v1")


class Mapping(_MappingProperties, TypedNode):
    """This represents the reading format of mapping.

    It is used to when data is read from CDF.

    The mapping between asset-centric and data modeling resources

    Args:
        space: The space where the node is located.
        external_id: The external id of the mapping.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        resource_type: The resource type field.
        id_: The id field.
        data_set_id: The data set id field.
        classic_external_id: The classic external id field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        resource_type: Literal["asset", "event", "file", "sequence", "timeseries"],
        id_: int,
        data_set_id: int | None = None,
        classic_external_id: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.resource_type = resource_type
        self.id_ = id_
        self.data_set_id = data_set_id
        self.classic_external_id = classic_external_id

    def as_asset_centric_id(self) -> AssetCentricId:
        """Return the AssetCentricId representation of the mapping."""
        return AssetCentricId(
            resource_type=self.resource_type,
            id_=self.id_,
        )
