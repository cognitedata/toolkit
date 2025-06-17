import sys
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import Field, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import BaseModelResource

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class DataModelInfo(BaseModelResource):
    space: str = Field(description="Space of the Data Model.")
    external_id: str = Field(description="External ID of the Data Model.")
    version: str = Field(description="Version of the Data Model.")
    destination_type: str = Field(description="External ID of the type(view) in the data model.")
    destination_relationship_from_type: str | None = Field(
        default=None, description="Property Identifier of the connection definition in destinationType."
    )


class ViewInfo(BaseModelResource):
    space: str = Field(description="Space of the view.")
    external_id: str = Field(description="External ID of the view.")
    version: str = Field(description="Version of the view.")


class EdgeType(BaseModelResource):
    space: str = Field(description="Space of the type.")
    external_id: str = Field(description="External ID of the type.")


class Destination(BaseModelResource):
    _destination_type: ClassVar[str]
    type: str

    @model_validator(mode="wrap")
    @classmethod
    def find_destination_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, Destination):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid destination data '{type(data)}' expected dict")

        if cls is not Destination:
            return handler(data)

        dest_type = data.get("type")
        if dest_type is None:
            raise ValueError("Missing 'type' field in destination data")
        if dest_type not in _DESTINATION_CLASS_BY_TYPE:
            raise ValueError(
                f"invalid destination type '{dest_type}'. Expected one of {humanize_collection(_DESTINATION_CLASS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _DESTINATION_CLASS_BY_TYPE[dest_type]
        return cast(Self, cls_.model_validate(data))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_destination(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        return serialized_data


class StandardDataSource(Destination):
    _destination_type: ClassVar[str]
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
    _destination_type = "assets"
    type: Literal["assets"] = "assets"


class EventsDataSource(StandardDataSource):
    _destination_type = "events"
    type: Literal["events"] = "events"


class AssetHierarchyDataSource(StandardDataSource):
    _destination_type = "asset_hierarchy"
    type: Literal["asset_hierarchy"] = "asset_hierarchy"


class DataPointsDataSource(StandardDataSource):
    _destination_type = "datapoints"
    type: Literal["datapoints"] = "datapoints"


class StringDataPointsDataSource(StandardDataSource):
    _destination_type = "string_datapoints"
    type: Literal["string_datapoints"] = "string_datapoints"


class TimeSeriesDataSource(StandardDataSource):
    _destination_type = "timeseries"
    type: Literal["timeseries"] = "timeseries"


class SequencesDataSource(StandardDataSource):
    _destination_type = "sequences"
    type: Literal["sequences"] = "sequences"


class FilesDataSource(StandardDataSource):
    _destination_type = "files"
    type: Literal["files"] = "files"


class LabelsDataSource(StandardDataSource):
    _destination_type = "labels"
    type: Literal["labels"] = "labels"


class RelationshipsDataSource(StandardDataSource):
    _destination_type = "relationships"
    type: Literal["relationships"] = "relationships"


class DataSetsDataSource(StandardDataSource):
    _destination_type = "data_sets"
    type: Literal["data_sets"] = "data_sets"


class DataModelSource(Destination):
    _destination_type = "instances"
    type: Literal["instances"] = "instances"
    data_model: DataModelInfo = Field(description="Target data model info.")
    instance_space: str | None = Field(None, description="The space where the instances will be created.")


class ViewDataSource(Destination):
    _destination_type: ClassVar[str]
    type: Literal["nodes", "edges"]
    view: ViewInfo | None = Field(default=None, description="Target view info.")
    edge_type: EdgeType | None = Field(default=None, description="Target type of the connection definition.")
    instance_space: str | None = Field(
        default=None, description="The space where the instances(nodes/edges) will be created."
    )


class NodeViewDataSource(ViewDataSource):
    _destination_type = "nodes"
    type: Literal["nodes"] = "nodes"


class EdgeViewDataSource(ViewDataSource):
    _destination_type = "edges"
    type: Literal["edges"] = "edges"


class RawDataSource(Destination):
    _destination_type = "raw"
    type: Literal["raw"] = "raw"
    database: str = Field(description="The database name.")
    table: str = Field(description="The table name.")


class SequenceRowDataSource(Destination):
    _destination_type = "sequence_rows"
    type: Literal["sequence_rows"] = "sequence_rows"
    external_id: str = Field(description="The externalId of sequence.")


def get_all_destination_leaf_classes(base_class: type[Destination]) -> list:
    subclasses = base_class.__subclasses__()
    result = []

    if not subclasses:
        if base_class is not Destination:
            result.append(base_class)
    else:
        for subclass in subclasses:
            result.extend(get_all_destination_leaf_classes(subclass))

    return result


_DESTINATION_CLASS_BY_TYPE: MappingProxyType[str, type[Destination]] = MappingProxyType(
    {
        cls._destination_type: cls
        for cls in get_all_destination_leaf_classes(Destination)
        if hasattr(cls, "_destination_type") and cls._destination_type is not None
    }
)

ALL_DESTINATION_TYPES = sorted(_DESTINATION_CLASS_BY_TYPE)
