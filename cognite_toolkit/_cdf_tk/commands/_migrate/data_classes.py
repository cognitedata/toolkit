from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Literal

from cognite.client.data_classes._base import (
    T_WritableCogniteResource,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import InstanceApply, NodeId, ViewId
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, field_validator, model_validator

from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.storageio._data_classes import ModelList
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricKind, AssetCentricType, JsonVal


class MigrationMapping(BaseModel, alias_generator=to_camel_case, extra="ignore", populate_by_name=True):
    """The mapping between an asset-centric ID and a data modeling instance ID.
    Args
        resource_type (str): The asset-centric type of the resource (e.g., "asset", "event", "timeseries").
        instance_id (NodeId): The target NodeId in data modeling.
        id (int): The asset-centric ID of the resource.
        data_set_id (int | None): The data set ID of the resource. This is used to validate access to the resource.
        ingestion_view (str | None): The ingestion view name. This is the view mapping that will be used to
            ingest the resource into data modeling.
        preferred_consumer_view (ViewId | None): The preferred consumer view for the resource. This is used in
           for example, the Canvas migration to determine which view to use for the resource.
    """

    resource_type: AssetCentricType
    instance_id: NodeId
    id: int
    data_set_id: int | None = None
    ingestion_view: str | None = None
    preferred_consumer_view: ViewId | None = None

    def get_ingestion_view(self) -> str:
        """Get the ingestion view for the mapping. If not specified, return the default ingestion view."""
        if self.ingestion_view:
            return self.ingestion_view

        default_mappings = create_default_mappings()
        for mapping in default_mappings:
            if mapping.resource_type == self.resource_type:
                return mapping.external_id
        raise ToolkitValueError(f"No default ingestion view specified for resource type '{self.resource_type}'")

    def as_asset_centric_id(self) -> AssetCentricId:
        return AssetCentricId(resource_type=self.resource_type, id_=self.id)

    @model_validator(mode="before")
    def _handle_flat_dict(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values

        if "space" in values and "externalId" in values:
            values["instanceId"] = {"space": values.pop("space"), "externalId": values.pop("externalId")}
        if "consumerViewSpace" in values and "consumerViewExternalId" in values:
            consumer_view = {
                "space": values.pop("consumerViewSpace"),
                "externalId": values.pop("consumerViewExternalId"),
            }
            if "consumerViewVersion" in values:
                consumer_view["version"] = values.pop("consumerViewVersion")
            values["preferredConsumerView"] = consumer_view
        return values

    @field_validator("data_set_id", "ingestion_view", mode="before")
    def _empty_string_to_none(cls, v: Any) -> Any:
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("preferred_consumer_view", mode="before")
    def _validate_preferred_consumer_view(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return ViewId.load(v)
        return v

    @field_validator("instance_id", mode="before")
    def _validate_instance_id(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return NodeId.load(v)
        return v


class MigrationMappingList(ModelList[MigrationMapping]):
    @classmethod
    def _get_base_model_cls(cls) -> type[MigrationMapping]:
        return MigrationMapping

    @classmethod
    def _required_header_names(cls) -> set[str]:
        return {"id", "space", "externalId"}

    @classmethod
    def _optional_header_names(cls) -> set[str]:
        return {"dataSetId", "ingestionView", "consumerViewSpace", "consumerViewExternalId", "consumerViewVersion"}

    def get_ids(self) -> list[int]:
        """Return a list of IDs from the migration mappings."""
        return [mapping.id for mapping in self]

    def as_node_ids(self) -> list[NodeId]:
        """Return a list of NodeIds from the migration mappings."""
        return [mapping.instance_id for mapping in self]

    def spaces(self) -> set[str]:
        """Return a set of spaces from the migration mappings."""
        return {mapping.instance_id.space for mapping in self}

    def as_pending_ids(self) -> list[PendingInstanceId]:
        return [PendingInstanceId(pending_instance_id=mapping.instance_id, id=mapping.id) for mapping in self]

    def get_data_set_ids(self) -> set[int]:
        """Return a list of data set IDs from the migration mappings."""
        return {mapping.data_set_id for mapping in self if mapping.data_set_id is not None}

    def as_mapping_by_id(self) -> dict[int, MigrationMapping]:
        """Return a mapping of IDs to MigrationMapping objects."""
        return {mapping.id: mapping for mapping in self}

    @classmethod
    def read_csv_file(cls, filepath: Path, resource_type: AssetCentricKind | None = None) -> "MigrationMappingList":
        if cls is not MigrationMappingList or resource_type is None:
            return super().read_csv_file(filepath)
        cls_by_resource_type: dict[str, type[MigrationMappingList]] = {
            "Assets": AssetMigrationMappingList,
            "TimeSeries": TimeSeriesMigrationMappingList,
            "FileMetadata": FileMigrationMappingList,
            "Events": EventMigrationMappingList,
        }
        if resource_type not in cls_by_resource_type:
            raise ToolkitValueError(
                f"Invalid resource type '{resource_type}'. Must be one of 'asset', 'timeseries', or 'file'."
            )
        return cls_by_resource_type[resource_type].read_csv_file(filepath, resource_type=None)


class AssetMapping(MigrationMapping):
    resource_type: Literal["asset"] = "asset"


class EventMapping(MigrationMapping):
    resource_type: Literal["event"] = "event"


class TimeSeriesMapping(MigrationMapping):
    resource_type: Literal["timeseries"] = "timeseries"


class FileMapping(MigrationMapping):
    resource_type: Literal["file"] = "file"


class AssetMigrationMappingList(MigrationMappingList):
    @classmethod
    def _get_base_model_cls(cls) -> type[AssetMapping]:
        return AssetMapping


class EventMigrationMappingList(MigrationMappingList):
    @classmethod
    def _get_base_model_cls(cls) -> type[EventMapping]:
        return EventMapping


class FileMigrationMappingList(MigrationMappingList):
    @classmethod
    def _get_base_model_cls(cls) -> type[FileMapping]:
        return FileMapping


class TimeSeriesMigrationMappingList(MigrationMappingList):
    @classmethod
    def _get_base_model_cls(cls) -> type[TimeSeriesMapping]:
        return TimeSeriesMapping


@dataclass
class AssetCentricMapping(Generic[T_WritableCogniteResource], WriteableCogniteResource[InstanceApply]):
    mapping: MigrationMapping
    resource: T_WritableCogniteResource

    def as_write(self) -> InstanceApply:
        raise NotImplementedError()

    def dump(self, camel_case: bool = True) -> dict[str, JsonVal]:
        mapping = self.mapping.model_dump(exclude_unset=True, by_alias=camel_case)
        # Ensure that resource type is always included, even if unset.
        mapping["resourceType" if camel_case else "resource_type"] = self.mapping.resource_type
        return {
            "mapping": mapping,
            "resource": self.resource.dump(camel_case=camel_case),
        }


class AssetCentricMappingList(
    WriteableCogniteResourceList[InstanceApply, AssetCentricMapping[T_WritableCogniteResource]]
):
    _RESOURCE: type = AssetCentricMapping

    def as_write(self) -> InstanceApplyList:
        return InstanceApplyList([item.as_write() for item in self])
