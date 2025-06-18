import csv
import sys
from abc import abstractmethod
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

    @abstractmethod
    def get_id(self) -> int | str:
        raise NotImplementedError()


@dataclass
class IdMigrationMapping(MigrationMapping):
    id: int
    data_set_id: int | None = None

    def get_id(self) -> int:
        return self.id


@dataclass
class ExternalIdMigrationMapping(MigrationMapping):
    external_id: str
    data_set_id: int | None = None

    def get_id(self) -> str:
        return self.external_id


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
        return [mapping.id for mapping in self if isinstance(mapping, IdMigrationMapping)]

    def get_external_ids(self) -> list[str]:
        """Return a list of external IDs from the migration mappings."""
        return [mapping.external_id for mapping in self if isinstance(mapping, ExternalIdMigrationMapping)]

    def as_node_ids(self) -> list[NodeId]:
        """Return a list of NodeIds from the migration mappings."""
        return [mapping.instance_id for mapping in self]

    def spaces(self) -> set[str]:
        """Return a set of spaces from the migration mappings."""
        return {mapping.instance_id.space for mapping in self}

    def as_pending_ids(self) -> list[PendingInstanceId]:
        return [
            PendingInstanceId(
                pending_instance_id=mapping.instance_id,
                id=mapping.id if isinstance(mapping, IdMigrationMapping) else None,
                external_id=mapping.external_id if isinstance(mapping, ExternalIdMigrationMapping) else None,
            )
            for mapping in self
        ]

    def get_data_set_ids(self) -> set[int]:
        """Return a list of data set IDs from the migration mappings."""
        return {
            mapping.data_set_id
            for mapping in self
            if isinstance(mapping, IdMigrationMapping | ExternalIdMigrationMapping) and mapping.data_set_id is not None
        }

    def as_mapping_by_id(self) -> dict[int | str, MigrationMapping]:
        """Return a mapping of IDs to MigrationMapping objects."""
        return {mapping.get_id(): mapping for mapping in self}

    @classmethod
    def read_mapping_file(cls, mapping_file: Path) -> Self:
        if not mapping_file.exists():
            raise ToolkitFileNotFoundError(f"Mapping file {mapping_file} does not exist.")
        if mapping_file.suffix != ".csv":
            raise ToolkitValueError(f"Mapping file {mapping_file} must be a CSV file.")
        with mapping_file.open(mode="r") as f:
            csv_file = csv.reader(f)
            header = next(csv_file, None)
            header = cls._validate_csv_header(header)
            if header[0] == "id":
                return cls._read_id_mapping(csv_file)
            else:  # header[0] == "externalId":
                return cls._read_external_id_mapping(csv_file)

    @classmethod
    def _validate_csv_header(cls, header: list[str] | None) -> list[str]:
        if header is None:
            raise ToolkitValueError("Mapping file is empty")
        errors: list[str] = []
        if len(header) < 3:
            errors.append("Mapping file must have at least 3 columns: id/externalId, space, externalId.")
        if len(header) >= 5:
            errors.append("Mapping file must have at most 4 columns: id/externalId, dataSetId, space, externalId.")
        if header[0] not in {"id", "externalId"}:
            errors.append("First column must be 'id' or 'externalId'.")
        if len(header) == 4 and header[1] != "dataSetId":
            errors.append("If there are 4 columns, the second column must be 'dataSetId'.")
        if header[-2:] != ["space", "externalId"]:
            errors.append("Last two columns must be 'space' and 'externalId'.")
        if errors:
            error_str = "\n - ".join(errors)
            raise ToolkitValueError(f"Invalid mapping file header:\n - {error_str}")
        return header

    @classmethod
    def _read_id_mapping(cls, csv_file: Iterator[list[str]]) -> Self:
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
                IdMigrationMapping(resource_type="timeseries", id=id_, instance_id=instance_id, data_set_id=data_set_id)
            )
        return mappings

    @classmethod
    def _read_external_id_mapping(cls, csv_file: Iterator[list[str]]) -> Self:
        """Read a CSV file with external ID mappings."""
        mappings = cls()
        for row in csv_file:
            external_id = row[0]
            data_set_id = int(row[1]) if len(row) == 4 and row[1] else None
            instance_id = NodeId(*row[-2:])
            mappings.append(
                ExternalIdMigrationMapping(
                    resource_type="timeseries",
                    external_id=external_id,
                    instance_id=instance_id,
                    data_set_id=data_set_id,
                )
            )
        return mappings
