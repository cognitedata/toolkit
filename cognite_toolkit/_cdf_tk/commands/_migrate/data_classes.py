import csv
import sys
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import SupportsIndex, overload

from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileNotFoundError,
    ToolkitValueError,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class MigrationMapping:
    resource_type: str
    instance_id: NodeId
    id: int
    data_set_id: int | None = None


class MigrationMappingList(list, Sequence[MigrationMapping]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[MigrationMapping] | None = None) -> None:
        super().__init__(collection or [])

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
        with mapping_file.open(mode="r", encoding="utf-8-sig") as f:
            csv_file = csv.reader(f)
            header = next(csv_file, None)
            cls._validate_csv_header(header)
            return cls._read_migration_mapping(csv_file, resource_type)

    @classmethod
    def _validate_csv_header(cls, header: list[str] | None) -> list[str]:
        if header is None:
            raise ToolkitValueError("Mapping file is empty")
        errors: list[str] = []
        if len(header) < 3:
            errors.append(
                f"Mapping file must have at least 3 columns: id, space, externalId. Got {len(header)} columns."
            )
        if len(header) >= 5:
            errors.append(
                "Mapping file must have at most 4 columns: "
                f"id, dataSetId, space, externalId. Got {len(header)} columns."
            )
        if len(header) >= 1 and header[0] != "id":
            errors.append(f"First column must be 'id'. Got {header[0]!r}.")
        if len(header) == 4 and header[1] != "dataSetId":
            errors.append(f"If there are 4 columns, the second column must be 'dataSetId'. Got {header[1]!r}.")
        if len(header) >= 2 and header[-2:] != ["space", "externalId"]:
            errors.append(f"Last two columns must be 'space' and 'externalId'. Got {header[-2]!r} and {header[-1]!r}.")
        if errors:
            error_str = "\n - ".join(errors)
            raise ToolkitValueError(f"Invalid mapping file header:\n - {error_str}")
        return header

    @classmethod
    def _read_migration_mapping(cls, csv_file: Iterator[list[str]], resource_type: str) -> Self:
        """Read a CSV file with ID mappings."""
        mappings = cls()
        for no, row in enumerate(csv_file, 1):
            try:
                id_ = int(row[0])
                data_set_id = int(row[1]) if len(row) == 4 and row[1] else None
            except ValueError as e:
                raise ToolkitValueError(
                    f"Invalid ID or dataSetId in row {no}: {row}. ID and dataSetId must be integers."
                ) from e
            instance_id = NodeId(*row[-2:])
            mappings.append(
                MigrationMapping(resource_type=resource_type, id=id_, instance_id=instance_id, data_set_id=data_set_id)
            )
        return mappings
