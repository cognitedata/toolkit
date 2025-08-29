from collections.abc import Collection, Iterator, Sequence
from pathlib import Path
from typing import SupportsIndex, overload

from cognite.client.data_classes.data_modeling import EdgeId, NodeId
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader, SchemaColumn


class InstanceIdList(list, Sequence[InstanceId]):
    REQUIRED_HEADER = (
        SchemaColumn("space", "string"),
        SchemaColumn("externalId", "string"),
        SchemaColumn("instanceType", "string"),
    )

    # Implemented to get correct type hints
    def __init__(
        self, collection: Collection[InstanceId] | None = None, invalid_rows: dict[int, list[str]] | None = None
    ) -> None:
        super().__init__(collection or [])
        self.invalid_rows = invalid_rows or {}

    def __iter__(self) -> Iterator[InstanceId]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> InstanceId: ...

    @overload
    def __getitem__(self, index: slice) -> "InstanceIdList": ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> "InstanceId | InstanceIdList":
        if isinstance(index, slice):
            return InstanceIdList(super().__getitem__(index))
        return super().__getitem__(index)

    @classmethod
    def read_csv_file(cls, filepath: Path) -> "InstanceIdList":
        # We only need to read one row to get the header
        schema = CSVReader.sniff_schema(filepath, sniff_rows=1)
        cls._validate_schema(schema)
        reader = CSVReader(input_file=filepath)
        instance_ids: list[InstanceId] = []
        invalid_rows: dict[int, list[str]] = {}
        for row_no, row in enumerate(reader.read_chunks_unprocessed(), start=1):
            space, external_id, instance_type = row["space"], row["externalId"], row["instanceType"]
            errors: list[str] = []
            if space.strip() == "":
                errors.append("Space is empty.")
            if external_id.strip() == "":
                errors.append("External ID is empty.")

            instance_id: InstanceId | None = None
            if instance_type == "node":
                instance_id = NodeId.load(row)
            elif instance_type == "edge":
                instance_id = EdgeId.load(row)
            else:
                errors.append(f"Unknown instance type {instance_type!r}, expected 'node' or 'edge'.")
            if errors:
                invalid_rows[row_no] = errors

            if instance_id and not errors:
                instance_ids.append(instance_id)

        return cls(instance_ids, invalid_rows)

    @classmethod
    def _validate_schema(cls, schema: list[SchemaColumn]) -> None:
        expected = {col.name for col in cls.REQUIRED_HEADER}
        actual = {col.name for col in schema}
        if missing_columns := expected - actual:
            raise ToolkitValueError(f"Missing required columns: {humanize_collection(missing_columns)}")
