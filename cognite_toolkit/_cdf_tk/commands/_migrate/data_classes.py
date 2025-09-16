import sys
from collections.abc import Collection, Iterator, Sequence
from pathlib import Path
from typing import Any, SupportsIndex, overload

from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, field_validator
from pydantic_core import ValidationError
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader, SchemaColumn

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class MigrationMapping(BaseModel, alias_generator=to_camel_case, extra="ignore"):
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

    resource_type: str
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
        return AssetCentricId(resource_type=self.resource_type, id_=self.id)  # type: ignore[arg-type]

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


class MigrationMappingList(list, Sequence[MigrationMapping]):
    REQUIRED_HEADER = (
        SchemaColumn("id", "integer"),
        SchemaColumn("space", "string"),
        SchemaColumn("externalId", "string"),
    )
    OPTIONAL_HEADER = (
        SchemaColumn("dataSetId", "integer"),
        SchemaColumn("ingestionView", "string"),
        SchemaColumn("consumerViewSpace", "string"),
        SchemaColumn("consumerViewExternalId", "string"),
        SchemaColumn("consumerViewVersion", "string"),
    )

    def __init__(
        self,
        collection: Collection[MigrationMapping] | None = None,
        error_by_row_no: dict[int, ValidationError] | None = None,
    ) -> None:
        super().__init__(collection or [])
        self.error_by_row_no = error_by_row_no or {}

    def __iter__(self) -> Iterator[MigrationMapping]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> MigrationMapping: ...

    @overload
    def __getitem__(self, index: slice) -> "MigrationMappingList": ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> "MigrationMapping | MigrationMappingList":
        if isinstance(index, slice):
            return MigrationMappingList(super().__getitem__(index))
        return super().__getitem__(index)

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
    def read_mapping_file(cls, mapping_file: Path, resource_type: str, console: Console | None = None) -> Self:
        # We only validate the schema heading here
        schema = CSVReader.sniff_schema(mapping_file, sniff_rows=1)
        cls._validate_csv_header(schema, console)
        mappings, errors = cls._read_migration_mapping(mapping_file, resource_type)
        return cls(mappings, errors)

    @classmethod
    def _validate_csv_header(cls, schema: list[SchemaColumn], console: Console | None) -> None:
        """Validate the CSV header against the required and optional columns."""
        column_names = {col.name for col in schema}
        missing_required = [col.name for col in cls.REQUIRED_HEADER if col.name not in column_names]
        if missing_required:
            raise ToolkitValueError(
                f"Missing required columns in mapping file: {humanize_collection(missing_required)}."
            )
        expected_names = {col.name for col in cls.REQUIRED_HEADER + cls.OPTIONAL_HEADER}
        ignored = [col.name for col in schema if col.name not in expected_names]
        if ignored:
            LowSeverityWarning(
                f"Ignoring unexpected columns in mapping file: {humanize_collection(ignored)}"
            ).print_warning(console=console)

    @classmethod
    def _read_migration_mapping(
        cls, csv_file: Path, resource_type: str
    ) -> tuple[list[MigrationMapping], dict[int, ValidationError]]:
        """Read a CSV file with ID mappings."""
        mappings: list[MigrationMapping] = []
        errors: dict[int, ValidationError] = {}
        chunk: dict[str, Any]

        def _extract_and_pop(chunk_: dict[str, Any], key_mapping: dict[str, str]) -> dict[str, str]:
            """Pops keys from chunk and returns a new dict with mapped keys."""
            return {dest: chunk_.pop(src) for src, dest in key_mapping.items() if src in chunk_}

        instance_id_mapping = {"space": "space", "externalId": "externalId"}
        consumer_view_mapping = {
            "consumerViewSpace": "space",
            "consumerViewExternalId": "externalId",
            "consumerViewVersion": "version",
        }

        for row_no, chunk in enumerate(CSVReader(csv_file).read_chunks_unprocessed(), 1):
            # Prepare for parsing
            if instance_id := _extract_and_pop(chunk, instance_id_mapping):
                # MyPy does not respect the chunk annotation above, it uses dict[str, str] from the CSVReader.
                chunk["instanceId"] = NodeId.load(instance_id)  # type: ignore[assignment]
            if consumer_view := _extract_and_pop(chunk, consumer_view_mapping):
                chunk["preferredConsumerView"] = ViewId.load(consumer_view)  # type: ignore[assignment]
            chunk["resourceType"] = resource_type
            try:
                mapping = MigrationMapping.model_validate(chunk)
            except ValidationError as e:
                errors[row_no] = e
                continue
            mappings.append(mapping)
        return mappings, errors
