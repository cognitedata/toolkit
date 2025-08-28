from collections.abc import Collection, Iterator, Sequence
from pathlib import Path
from typing import SupportsIndex, overload

from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader, SchemaColumn


class InstanceIdList(list, Sequence[InstanceId]):
    REQUIRED_HEADER = (SchemaColumn("space", "string"), SchemaColumn("externalId", "string"))

    # Implemented to get correct type hints
    def __init__(self, collection: Collection[InstanceId] | None = None) -> None:
        super().__init__(collection or [])

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
    def from_file(cls, filepath: Path) -> "InstanceIdList":
        # We only need to read one row to get the header
        schema = CSVReader.sniff_schema(filepath, sniff_rows=1)
        cls._validate_schema(schema)
        # We force string type for all columns no matter what was inferred by the sniffing
        reader = CSVReader(input_file=filepath, schema=cls.REQUIRED_HEADER, keep_failed_cells=True)
        instance_ids: list[InstanceId] = []
        for row in reader.read_chunks():
            instance_id = InstanceId.load(row)  # type: ignore[arg-type]
            instance_ids.append(instance_id)
        return cls(instance_ids)

    @classmethod
    def _validate_schema(cls, schema: list[SchemaColumn]) -> None:
        expected = {col.name for col in cls.REQUIRED_HEADER}
        actual = {col.name for col in schema}
        if missing_columns := expected - actual:
            raise ToolkitValueError(f"Missing required columns: {humanize_collection(missing_columns)}")
