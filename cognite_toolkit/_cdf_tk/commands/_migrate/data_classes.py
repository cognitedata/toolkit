import sys
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, SupportsIndex, overload

from cognite.client.data_classes.data_modeling import NodeId, ViewId

from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader, SchemaColumn

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class MigrationMapping:
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

    @classmethod
    def load_row(cls, data: dict[str, Any], resource_type: str) -> Self:
        """Load the MigrationMapping from a JSON-like structure."""
        space = data.get("consumerViewSpace")
        external_id = data.get("consumerViewExternalId")
        version = data.get("consumerViewVersion")
        preferred_consumer_view: ViewId | None = None
        if space and external_id and version:
            preferred_consumer_view = ViewId(space=str(space), external_id=str(external_id), version=str(version))

        return cls(
            resource_type=resource_type,
            instance_id=NodeId(str(data["space"]), str(data["externalId"])),
            id=int(data["id"]),
            data_set_id=int(data["dataSetId"]) if data.get("dataSetId") is not None else None,
            ingestion_view=str(data["ingestionView"]) if data.get("ingestionView") is not None else None,
            preferred_consumer_view=preferred_consumer_view,
        )


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
        self, collection: Collection[MigrationMapping] | None = None, failed_rows: dict[int, str] | None = None
    ) -> None:
        super().__init__(collection or [])
        self.failed_rows: dict[int, str] = failed_rows or {}

    # Implemented to get correct type hints
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
    def read_mapping_file(cls, mapping_file: Path, resource_type: str) -> Self:
        if not mapping_file.exists():
            raise ToolkitFileNotFoundError(f"Mapping file {mapping_file} does not exist.")
        if mapping_file.suffix != ".csv":
            raise ToolkitValueError(f"Mapping file {mapping_file} must be a CSV file.")

        schema = CSVReader.sniff_schema(mapping_file, sniff_rows=1000)
        schema = cls._ensure_version_column_is_string(schema)
        cls._validate_header(schema)

        mappings: list[MigrationMapping] = []
        failed_rows: dict[int, str] = {}
        for row_no, row in enumerate(CSVReader(mapping_file, schema=schema).read_chunks(), start=1):
            try:
                mapping = MigrationMapping.load_row(row, resource_type)
            except KeyError as e:
                failed_rows[row_no] = f"Row {row_no} in mapping file is missing required fields: {e.args[0]}"
                continue
            except TypeError:
                failed_rows[row_no] = f"Row {row_no} in mapping file has invalid data types."
                continue
            mappings.append(mapping)
        return cls(mappings, failed_rows=failed_rows)

    @classmethod
    def _validate_header(cls, schema: list[SchemaColumn]) -> None:
        errors: list[str] = []
        required_names = [col.name for col in cls.REQUIRED_HEADER]
        dtype_by_name = {col.name: col.type for col in schema}
        expected_dtype_by_name = {col.name: col.type for col in cls.REQUIRED_HEADER + cls.OPTIONAL_HEADER}
        if not schema:
            errors.append(
                f"Mapping file must have at least 3 columns: {humanize_collection(required_names, sort=False)}"
            )
        if missing := [col.name for col in cls.REQUIRED_HEADER if col.name not in dtype_by_name]:
            errors.append(
                f"Mapping file must have the following columns: {humanize_collection(required_names, sort=False)}. "
                f"Missing: {humanize_collection(missing, sort=False)}."
            )
        if wrong_types := [
            (col, expected_dtype_by_name[col.name])
            for col in schema
            if col.name in expected_dtype_by_name and col.type != expected_dtype_by_name[col.name]
        ]:
            types_str = humanize_collection(
                [f"{col.name} (got={col.type!r},expected={expected!r})" for col, expected in wrong_types]
            )
            errors.append(f"Mapping file has incorrect data types for columns: {types_str}.")

        if errors:
            error_str = "\n - ".join(errors)
            raise ToolkitValueError(f"Invalid file schema:\n - {error_str}")

    @classmethod
    def _ensure_version_column_is_string(cls, schema: list[SchemaColumn]) -> list[SchemaColumn]:
        """Versions are often given as integers, but we want to treat them as strings."""
        output: list[SchemaColumn] = []
        for col in schema:
            if col.name == "consumerViewVersion" and col.type != "string":
                output.append(SchemaColumn(name=col.name, type="string"))
            else:
                output.append(col)
        return output
