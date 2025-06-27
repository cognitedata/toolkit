from typing import Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class Scenes(BaseModelResource):
    external_id: str = Field(description="External ID of the scene.")
    space: str = Field(description="The space that the scene is in")


class DataModelID(BaseModelResource):
    external_id: str = Field(description="The external ID of the data model.")
    space: str = Field(description="The space of the data model.")
    version: str = Field(description="The version of the data model.")


class LocationFilterViewId(BaseModelResource):
    external_id: str = Field(description="The external ID of the view.")
    space: str = Field(description="The space of the view.")
    version: str = Field(description="The version of the view.")
    represents_entity: Literal["MAINTENANCE_ORDER", "OPERATION", "NOTIFICATION", "ASSET"] = Field(
        description="Represents entity type"
    )


class AssetCentricFields(BaseModelResource):
    data_set_external_ids: list[str] | None = Field(default=None, description="The list of data set external IDs")
    asset_subtree_external_ids: list[dict[Literal["externalId"], str]] | None = Field(
        default=None, description="External IDs of the asset."
    )
    external_id_prefix: str | None = Field(default=None, description="The external ID prefix")


class AssetCentricResourceData(AssetCentricFields):
    """ """


class AssetCentricResource(AssetCentricFields):
    assets: AssetCentricResourceData | None = Field(default=None, description="Asset resource type data")
    events: AssetCentricResourceData | None = Field(default=None, description="Event resource type data")
    timeseries: AssetCentricResourceData | None = Field(default=None, description="Timeseries resource type data")
    files: AssetCentricResourceData | None = Field(default=None, description="File resource type data")
    sequences: AssetCentricResourceData | None = Field(default=None, description="Sequence resource type data")


class LocationYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.")
    name: str = Field(description="The name of the location.")
    description: str | None = Field(default=None, description="The description of the data set.", max_length=255)
    parent_external_id: str | None = Field(default=None, description="The external ID of the parent location.")
    data_models: list[DataModelID] | None = Field(
        default=None, description="The data models associated with the location."
    )
    instance_spaces: list[str] | None = Field(default=None, description="The list of spaces that instances are in")
    scene: Scenes | None = Field(default=None, description="The scene config for the location.")
    asset_centric: AssetCentricResource | None = Field(
        default=None,
        description="The filter definition for asset centric resource types",
    )
    views: list[LocationFilterViewId] | None = Field(
        default=None, description="The views associated with the location."
    )
    data_modeling_type: Literal["HYBRID", "DATA_MODELING_ONLY"] | None = Field(
        default="HYBRID", description="The data modeling type"
    )
