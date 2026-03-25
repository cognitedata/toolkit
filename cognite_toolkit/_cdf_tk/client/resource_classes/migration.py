from typing import ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import WrappedInstanceResponseOnly
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricType, AssetCentricTypeExtended

INSTANCE_SOURCE_VIEW_ID = ViewId(space="cognite_migration", external_id="InstanceSource", version="v1")
CREATED_SOURCE_SYSTEM_VIEW_ID = ViewId(space="cognite_migration", external_id="CreatedSourceSystem", version="v1")
SPACE_SOURCE_VIEW_ID = ViewId(space="cognite_migration", external_id="SpaceSource", version="v1")


class AssetCentricId(Identifier):
    resource_type: AssetCentricTypeExtended
    id_: int = Field(alias="id")

    @property
    def id_value(self) -> int:
        """Generic name of the identifier.

        The AssetCentricExternalId has the same property. Thus, this means that these two
        classes can be used interchangeably when only the value of the identifier is needed, and not the type.
        """
        return self.id_

    def __str__(self) -> str:
        return f"{self.resource_type}(id={self.id_})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"resourceType-{self.resource_type}.id-{self.id_}"
        return f"{self.resource_type}.{self.id_}"


class InstanceSource(WrappedInstanceResponseOnly):
    """Pydantic model for reading InstanceSource nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = INSTANCE_SOURCE_VIEW_ID

    instance_type: Literal["node"] = "node"
    resource_type: AssetCentricType
    id_: int = Field(alias="id")
    data_set_id: int | None = None
    classic_external_id: str | None = None
    preferred_consumer_view_id: ViewId | None = None
    ingestion_view: dict[str, str] | None = None

    def as_asset_centric_id(self) -> AssetCentricId:
        return AssetCentricId(resource_type=self.resource_type, id_=self.id_)

    def consumer_view(self) -> ViewId:
        if self.preferred_consumer_view_id:
            return self.preferred_consumer_view_id
        if self.resource_type == "sequence":
            raise ValueError(f"Missing consumer view for sequence {self.external_id}.")
        external_id = {
            "asset": "CogniteAsset",
            "event": "CogniteActivity",
            "file": "CogniteFile",
            "timeseries": "CogniteTimeSeries",
        }[self.resource_type]
        return ViewId(space="cdf_cdm", external_id=external_id, version="v1")

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class CreatedSourceSystem(WrappedInstanceResponseOnly):
    """Pydantic model for reading CreatedSourceSystem nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = CREATED_SOURCE_SYSTEM_VIEW_ID
    instance_type: Literal["node"] = "node"
    source: str

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class SpaceSource(WrappedInstanceResponseOnly):
    """Pydantic model for reading SpaceSource nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = SPACE_SOURCE_VIEW_ID

    instance_type: Literal["node"] = "node"

    instance_space: str
    data_set_id: int
    data_set_external_id: str | None = None
