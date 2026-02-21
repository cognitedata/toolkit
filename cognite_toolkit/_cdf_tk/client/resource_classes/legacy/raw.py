import sys
from typing import Annotated, Any, TypeAlias

from pydantic import BeforeValidator, ConfigDict, PlainSerializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RawDatabase(BaseModelObject):
    model_config = ConfigDict(frozen=True)

    db_name: str

    def as_write(self) -> Self:
        return self


class RawTable(BaseModelObject):
    model_config = ConfigDict(frozen=True)

    db_name: str
    table_name: str

    def as_write(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"{self.db_name}.{self.table_name}"


def _unzip_pairs(value: Any) -> Any:
    """Convert parallel arrays [[a1, a2], [b1, b2]] into tuple of pairs ((a1, b1), (a2, b2)).

    Only transforms list inputs (from JSON); tuples are treated as already unpacked.
    """
    if isinstance(value, list) and len(value) == 2 and isinstance(value[0], list):
        return tuple(tuple(pair) for pair in zip(value[0], value[1]))
    return value


def _zip_pairs(value: Any) -> list[list[Any]]:
    """Convert tuple of pairs ((a1, b1), (a2, b2)) back into parallel arrays [[a1, a2], [b1, b2]]."""
    if not value:
        return [[], []]
    a, b = zip(*value)
    return [list(a), list(b)]


def _unzip_to_dict(value: Any) -> Any:
    """Convert parallel arrays [[k1, k2], [v1, v2]] into dict {k1: v1, k2: v2}.

    Only transforms list inputs (from JSON); dicts are returned as-is.
    """
    if isinstance(value, list) and len(value) == 2 and isinstance(value[0], list):
        return dict(zip(value[0], value[1]))
    return value


def _zip_from_dict(value: dict[str, int]) -> list[list[Any]]:
    """Convert dict {k1: v1, k2: v2} back into parallel arrays [[k1, k2], [v1, v2]]."""
    return [list(value.keys()), list(value.values())]


ZippedIntPairs = Annotated[
    tuple[tuple[int, int], ...],
    BeforeValidator(_unzip_pairs),
    PlainSerializer(_zip_pairs),
]

ZippedFloatIntPairs = Annotated[
    tuple[tuple[float, int], ...],
    BeforeValidator(_unzip_pairs),
    PlainSerializer(_zip_pairs),
]

ZippedStringIntDict = Annotated[
    dict[str, int],
    BeforeValidator(_unzip_to_dict),
    PlainSerializer(_zip_from_dict),
]


class StringProfile(BaseModelObject):
    length_range: tuple[int, int]
    distinct_count: int
    length_histogram: ZippedIntPairs
    value_counts: ZippedStringIntDict
    count: int


class NumberProfile(BaseModelObject):
    value_range: tuple[float, float]
    distinct_count: int
    value_counts: dict[str, int]
    histogram: ZippedFloatIntPairs
    count: int
    mean: float
    std: float
    median: float


class BooleanProfile(BaseModelObject):
    count: int
    true_count: int


class VectorProfile(BaseModelObject):
    count: int
    length_histogram: ZippedIntPairs
    length_range: tuple[int, int]


class ObjectProfile(BaseModelObject):
    count: int
    key_count_histogram: ZippedIntPairs
    key_count_range: tuple[int, int]


class RawProfileColumn(BaseModelObject):
    count: int
    null_count: int


class StringProfileColumn(RawProfileColumn):
    string: StringProfile


class NumberProfileColumn(RawProfileColumn):
    number: NumberProfile


class BooleanProfileColumn(RawProfileColumn):
    boolean: BooleanProfile


class VectorProfileColumn(RawProfileColumn):
    vector: VectorProfile


class ObjectProfileColumn(RawProfileColumn):
    object: ObjectProfile


class UnknownTypeProfileColumn(RawProfileColumn):
    """Fallback for unknown column types."""

    ...


KNOWN_COLUMN_TYPES: dict[str, type[RawProfileColumn]] = {
    "string": StringProfileColumn,
    "number": NumberProfileColumn,
    "boolean": BooleanProfileColumn,
    "vector": VectorProfileColumn,
    "object": ObjectProfileColumn,
}


def _handle_unknown_column(value: Any) -> Any:
    if isinstance(value, dict):
        for key, cls in KNOWN_COLUMN_TYPES.items():
            if key in value:
                return cls.model_validate(value)
        return UnknownTypeProfileColumn.model_validate(value)
    return value


ProfileColumnType = Annotated[
    StringProfileColumn
    | NumberProfileColumn
    | BooleanProfileColumn
    | VectorProfileColumn
    | ObjectProfileColumn
    | UnknownTypeProfileColumn,
    BeforeValidator(_handle_unknown_column),
]

RawProfileColumns: TypeAlias = dict[str, ProfileColumnType]


class RawProfileResults(BaseModelObject):
    """The results of a raw profile operation used with the /profiler/raw endpoint.

    This endpoint is undocumented, and thus it is hard to know exactly what each field means,
    so the following is based on the experimentation with the response of the endpoint.

    Args:
        row_count: The number of rows in the profile.
        columns: The columns in the profile, each column is a RawProfileColumn.
        is_complete: Whether the profiling operation is complete or not. If not complete, the results may be partial.

    """

    row_count: int
    columns: RawProfileColumns
    is_complete: bool

    @property
    def column_count(self) -> int:
        return len(self.columns)
