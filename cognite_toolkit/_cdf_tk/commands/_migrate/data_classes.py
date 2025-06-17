from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from typing import SupportsIndex, overload

from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId


@dataclass
class MigrationMapping:
    resource_type: str
    instance_id: NodeId


@dataclass
class IdMigrationMapping(MigrationMapping):
    id: int
    data_set_id: int | None = None


@dataclass
class ExternalIdMigrationMapping(MigrationMapping):
    external_id: str
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
