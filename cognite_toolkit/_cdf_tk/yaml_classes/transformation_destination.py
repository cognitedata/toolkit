from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource


class DataModelInfo(BaseModelResource):
    space: str = Field(
        description="Space of the Data Model.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(description="External ID of the Data Model.")
    version: str = Field(description="Version of the Data Model.")
    destination_type: str = Field(description="External ID of the type(view) in the data model.")
    destination_relationship_from_type: str | None = Field(
        default=None, description="Property Identifier of the connection definition in destinationType."
    )


class ViewInfo(BaseModelResource):
    space: str = Field(
        description="Space of the view.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(description="External ID of the view.")
    version: str = Field(description="Version of the view.")


class EdgeType(BaseModelResource):
    space: str = Field(
        description="Space of the type.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(description="External ID of the type.")


class AutoCreateOptions(BaseModelResource):
    start_nodes: bool | None = Field(
        default=None,
        description="Create missing start nodes when writing edges. Defaults to true.",
    )
    end_nodes: bool | None = Field(
        default=None,
        description="Create missing end nodes when writing edges. Defaults to true.",
    )
    direct_relations: bool | None = Field(
        default=None,
        description="Create missing direct-relation target nodes. Defaults to true.",
    )


class Destination(BaseModelResource):
    type: str


class StandardDataSource(Destination):
    type: Literal[
        "assets",
        "events",
        "asset_hierarchy",
        "datapoints",
        "string_datapoints",
        "timeseries",
        "sequences",
        "files",
        "labels",
        "relationships",
        "data_sets",
    ]


class AssetsDataSource(StandardDataSource):
    type: Literal["assets"] = "assets"


class EventsDataSource(StandardDataSource):
    type: Literal["events"] = "events"


class AssetHierarchyDataSource(StandardDataSource):
    type: Literal["asset_hierarchy"] = "asset_hierarchy"


class DataPointsDataSource(StandardDataSource):
    type: Literal["datapoints"] = "datapoints"


class StringDataPointsDataSource(StandardDataSource):
    type: Literal["string_datapoints"] = "string_datapoints"


class TimeSeriesDataSource(StandardDataSource):
    type: Literal["timeseries"] = "timeseries"


class SequencesDataSource(StandardDataSource):
    type: Literal["sequences"] = "sequences"


class FilesDataSource(StandardDataSource):
    type: Literal["files"] = "files"


class LabelsDataSource(StandardDataSource):
    type: Literal["labels"] = "labels"


class RelationshipsDataSource(StandardDataSource):
    type: Literal["relationships"] = "relationships"


class DataSetsDataSource(StandardDataSource):
    type: Literal["data_sets"] = "data_sets"


class DataModelSource(Destination):
    type: Literal["instances"] = "instances"
    data_model: DataModelInfo = Field(description="Target data model info.")
    instance_space: str | None = Field(
        None,
        description="The space where the instances will be created.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    auto_create: AutoCreateOptions | None = Field(
        default=None,
        description="Controls automatic creation of missing instance references.",
    )


class ViewDataSource(Destination):
    type: Literal["nodes", "edges"]
    view: ViewInfo | None = Field(default=None, description="Target view info.")
    edge_type: EdgeType | None = Field(default=None, description="Target type of the connection definition.")
    instance_space: str | None = Field(
        default=None,
        description="The space where the instances(nodes/edges) will be created.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    auto_create: AutoCreateOptions | None = Field(
        default=None,
        description="Controls automatic creation of missing instance references.",
    )


class NodeViewDataSource(ViewDataSource):
    type: Literal["nodes"] = "nodes"


class EdgeViewDataSource(ViewDataSource):
    type: Literal["edges"] = "edges"


class RawDataSource(Destination):
    type: Literal["raw"] = "raw"
    database: str = Field(description="The database name.")
    table: str = Field(description="The table name.")


class SequenceRowDataSource(Destination):
    type: Literal["sequence_rows"] = "sequence_rows"
    external_id: str = Field(description="The externalId of sequence.")


DestinationType = Annotated[
    AssetsDataSource
    | EventsDataSource
    | AssetHierarchyDataSource
    | DataPointsDataSource
    | StringDataPointsDataSource
    | TimeSeriesDataSource
    | SequencesDataSource
    | FilesDataSource
    | LabelsDataSource
    | RelationshipsDataSource
    | DataSetsDataSource
    | DataModelSource
    | NodeViewDataSource
    | EdgeViewDataSource
    | RawDataSource
    | SequenceRowDataSource,
    Field(discriminator="type"),
]
