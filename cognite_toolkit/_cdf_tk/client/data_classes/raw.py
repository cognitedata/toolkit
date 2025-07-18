import sys
from abc import abstractmethod
from collections.abc import ItemsView, Iterator, KeysView, Mapping, ValuesView
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, ClassVar, cast

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass(frozen=True)
class RawDatabase(WriteableCogniteResource):
    db_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(db_name=resource["dbName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"dbName" if camel_case else "db_name": self.db_name}

    def as_write(self) -> Self:
        return self


@dataclass(frozen=True)
class RawTable(WriteableCogniteResource):
    db_name: str
    table_name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(db_name=resource["dbName"], table_name=resource["tableName"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "dbName" if camel_case else "db_name": self.db_name,
            "tableName" if camel_case else "table_name": self.table_name,
        }

    def as_write(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"{self.db_name}.{self.table_name}"


class RawDatabaseList(WriteableCogniteResourceList[RawDatabase, RawDatabase]):
    _RESOURCE = RawDatabase

    def as_write(self) -> Self:
        return self


class RawTableList(WriteableCogniteResourceList[RawTable, RawTable]):
    _RESOURCE = RawTable

    def as_write(self) -> Self:
        return self


@dataclass
class RawProfileColumn(CogniteObject):
    _type: ClassVar[str]
    count: int
    null_count: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        for column_type, column_class in _RAW_PROFILE_COLUMN_BY_TYPE.items():
            if column_type in resource:
                return cast(Self, column_class._load_column(resource))
        return cast(Self, UnknownTypeProfileColumn._load_column(resource))

    @classmethod
    @abstractmethod
    def _load_column(cls, resource: dict[str, object]) -> Self:
        """Load a specific type of RawProfileColumn based on the resource data."""
        raise NotImplementedError("Subclasses must implement this method.")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "count": self.count,
            "nullCount" if camel_case else "null_count": self.null_count,
        }


@dataclass
class StringProfile(CogniteObject):
    length_range: tuple[int, int]
    distinct_count: int
    length_histogram: tuple[tuple[int, int], ...]  # List of (length, count) tuples
    value_counts: dict[str, int]
    count: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            length_range=tuple(resource["lengthRange"]),
            distinct_count=resource["distinctCount"],
            length_histogram=tuple((length, count) for length, count in zip(*resource["lengthHistogram"])),
            value_counts={value: count for value, count in zip(*resource["valueCounts"])},
            count=resource["count"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        length_histogram: list[list[int]] = [[], []]
        for length, count in self.length_histogram:
            length_histogram[0].append(length)
            length_histogram[1].append(count)
        value_counts: list[list[str | int]] = [[], []]
        for value, count in self.value_counts.items():
            value_counts[0].append(value)
            value_counts[1].append(count)

        return {
            "lengthRange" if camel_case else "length_range": list(self.length_range),
            "distinctCount" if camel_case else "distinct_count": self.distinct_count,
            "lengthHistogram" if camel_case else "length_histogram": length_histogram,
            "valueCounts" if camel_case else "value_counts": value_counts,
            "count": self.count,
        }


@dataclass
class NumberProfile(CogniteObject):
    value_range: tuple[float, float]
    distinct_count: int
    value_counts: dict[str, int]
    histogram: tuple[tuple[float, int], ...]
    count: int
    mean: float
    std: float
    median: float

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            value_range=tuple(resource["valueRange"]),
            distinct_count=resource["distinctCount"],
            value_counts=resource["valueCounts"],
            histogram=tuple((value, count) for value, count in zip(*resource["histogram"])),
            count=resource["count"],
            mean=resource["mean"],
            std=resource["std"],
            median=resource["median"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        histogram: list[list[float | int]] = [[], []]
        for value, count in self.histogram:
            histogram[0].append(value)
            histogram[1].append(count)
        return {
            "valueRange" if camel_case else "value_range": list(self.value_range),
            "distinctCount" if camel_case else "distinct_count": self.distinct_count,
            "valueCounts" if camel_case else "value_counts": self.value_counts,
            "histogram": histogram,
            "count": self.count,
            "mean": self.mean,
            "std": self.std,
            "median": self.median,
        }


@dataclass
class BooleanProfile(CogniteObject):
    count: int
    true_count: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            count=resource["count"],
            true_count=resource["trueCount"],
        )


@dataclass
class VectorProfile(CogniteObject):
    count: int
    length_histogram: tuple[tuple[int, int], ...]  # List of (length, count) tuples
    length_range: tuple[int, int]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            count=resource["count"],
            length_histogram=tuple((length, count) for length, count in zip(*resource["lengthHistogram"])),
            length_range=tuple(resource["lengthRange"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        length_histogram: list[list[int]] = [[], []]
        for length, count in self.length_histogram:
            length_histogram[0].append(length)
            length_histogram[1].append(count)
        return {
            "count": self.count,
            "lengthHistogram" if camel_case else "length_histogram": length_histogram,
            "lengthRange" if camel_case else "length_range": list(self.length_range),
        }


@dataclass
class ObjectProfile(CogniteObject):
    count: int
    key_count_histogram: tuple[tuple[int, int], ...]  # List of (key_count, count) tuples
    key_count_range: tuple[int, int]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            count=resource["count"],
            key_count_histogram=tuple((key_count, count) for key_count, count in zip(*resource["keyCountHistogram"])),
            key_count_range=tuple(resource["keyCountRange"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key_count_histogram: list[list[int]] = [[], []]
        for key_count, count in self.key_count_histogram:
            key_count_histogram[0].append(key_count)
            key_count_histogram[1].append(count)
        return {
            "count": self.count,
            "keyCountHistogram" if camel_case else "key_count_histogram": key_count_histogram,
            "keyCountRange" if camel_case else "key_count_range": list(self.key_count_range),
        }


@dataclass
class StringProfileColumn(RawProfileColumn):
    _type: ClassVar[str] = "string"
    string: StringProfile

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            string=StringProfile._load(resource["string"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["string"] = self.string.dump(camel_case=camel_case)
        return data


@dataclass
class NumberProfileColumn(RawProfileColumn):
    _type: ClassVar[str] = "number"
    number: NumberProfile

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            number=NumberProfile._load(resource["number"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["number"] = self.number.dump(camel_case=camel_case)
        return data


@dataclass
class BooleanProfileColumn(RawProfileColumn):
    _type: ClassVar[str] = "boolean"
    boolean: BooleanProfile

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            boolean=BooleanProfile._load(resource["boolean"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["boolean"] = self.boolean.dump(camel_case=camel_case)
        return data


@dataclass
class VectorProfileColumn(RawProfileColumn):
    _type: ClassVar[str] = "vector"
    vector: VectorProfile

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            vector=VectorProfile._load(resource["vector"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["vector"] = self.vector.dump(camel_case=camel_case)
        return data


@dataclass
class ObjectProfileColumn(RawProfileColumn):
    _type: ClassVar[str] = "object"
    object: ObjectProfile

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            object=ObjectProfile._load(resource["object"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data["object"] = self.object.dump(camel_case=camel_case)
        return data


@dataclass
class UnknownTypeProfileColumn(RawProfileColumn):
    data: dict[str, object]

    @classmethod
    def _load_column(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            null_count=resource["nullCount"],
            data={k: v for k, v in resource.items() if k not in {"count", "nullCount"}},
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        data.update(self.data)
        return data


class RawProfileColumns(dict, Mapping[str, RawProfileColumn], CogniteObject):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        columns = {name: RawProfileColumn._load(column_data) for name, column_data in resource.items()}
        return cls(columns)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {name: column.dump(camel_case=camel_case) for name, column in self.items()}

    # The methods are overloads to provide type hints for the methods.
    def items(self) -> ItemsView[str, RawProfileColumn]:  # type: ignore[override]
        return super().items()

    def keys(self) -> KeysView[str]:  # type: ignore[override]
        return super().keys()

    def values(self) -> ValuesView[RawProfileColumn]:  # type: ignore[override]
        return super().values()

    def __iter__(self) -> Iterator[str]:
        yield from super().__iter__()

    def __getitem__(self, column_name: str) -> RawProfileColumn:
        return super().__getitem__(column_name)


class RawProfileResults(CogniteResource):
    """The results of a raw profile operation used with the /profiler/raw endpoint.

    This endpoint is undocumented, and thus it is hard to know exactly what each field means,
    so the following is based on the experimentation with the response of the endpoint.

    Args:
        row_count: The number of rows in the profile.
        columns: The columns in the profile, each column is a RawProfileColumn.
        is_complete: Whether the profiling operation is complete or not. If not complete, the results may be partial.

    """

    def __init__(
        self,
        row_count: int,
        columns: RawProfileColumns,
        is_complete: bool,
    ) -> None:
        self.row_count = row_count
        self.columns = columns
        self.is_complete = is_complete

    @property
    def column_count(self) -> int:
        return len(self.columns)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            row_count=resource["rowCount"],
            columns=RawProfileColumns._load(resource["columns"]),
            is_complete=resource["isComplete"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "rowCount" if camel_case else "row_count": self.row_count,
            "columns": self.columns.dump(camel_case=camel_case),
            "isComplete" if camel_case else "is_complete": self.is_complete,
        }


_RAW_PROFILE_COLUMN_BY_TYPE: MappingProxyType[str, type[RawProfileColumn]] = MappingProxyType(
    # MyPy fails to understand that all subclasses of RawProfileColumn are concrete classes.
    {c._type: c for c in RawProfileColumn.__subclasses__() if not issubclass(c, UnknownTypeProfileColumn)}  # type: ignore[type-abstract]
)
