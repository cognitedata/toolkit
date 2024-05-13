from __future__ import annotations

import typing
from collections.abc import Hashable, Iterable, MutableSet
from dataclasses import dataclass
from functools import total_ordering

# Ruff wants to get AbstractSet from collections.abc, but mypy does not allow it.
from typing import AbstractSet, Generic, TypeVar, final  # noqa: UP035

from cognite.client.utils._text import to_camel_case

from .constants import SINGLETONS


@total_ordering
@dataclass(frozen=True)
class Parameter:
    path: tuple[str | int, ...]

    def __lt__(self, other: Parameter) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path < other.path

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path == other.path

    @property
    def has_any_type(self) -> bool:
        return any(name in SINGLETONS for name in self.path)


@final
@dataclass(frozen=True)
class ParameterSpec(Parameter):
    types: frozenset[str]
    is_required: bool
    _is_nullable: bool | None = None

    @property
    def is_nullable(self) -> bool:
        return self._is_nullable or not self.is_required

    # The eq, ne, and hash methods must be implemented for each subclass to get the set operations to work correctly.
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path == other.path

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path != other.path

    def __hash__(self) -> int:
        return hash(self.path)


@final
@dataclass(frozen=True)
class ParameterValue(Parameter):
    type: str
    value: str | int | float | bool | None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path == other.path

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path != other.path

    def __hash__(self) -> int:
        return hash(self.path)


T_Parameter = TypeVar("T_Parameter", bound=Parameter)


class ParameterSet(Hashable, MutableSet, Generic[T_Parameter]):
    def __init__(self, iterable: Iterable[T_Parameter] = ()) -> None:
        self.data: set[T_Parameter] = set(iterable)

    @property
    def has_any_type(self) -> bool:
        return any(parameter.has_any_type for parameter in self)

    def subset_any_type(self) -> ParameterSet[T_Parameter]:
        return type(self)(parameter for parameter in self if parameter.has_any_type)

    def subset(self, path: tuple[str | int, ...] | int) -> ParameterSet[T_Parameter]:
        if isinstance(path, tuple):
            return type(self)(parameter for parameter in self if parameter.path[: len(path)] == path)
        elif isinstance(path, int):
            return type(self)(parameter for parameter in self if len(parameter.path) <= path)
        raise TypeError(f"Expected tuple or int, got {type(path)}")

    def __hash__(self) -> int:
        return hash(self.data)

    def __contains__(self, value: object) -> bool:
        return value in self.data

    def __iter__(self) -> typing.Iterator[T_Parameter]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return repr(self.data)

    def add(self, item: T_Parameter) -> None:
        self.data.add(item)

    def discard(self, item: T_Parameter) -> None:
        self.data.discard(item)

    def update(self, other: ParameterSet[T_Parameter]) -> None:
        self.data.update(other.data)

    def difference(self, other: ParameterSet[T_Parameter]) -> ParameterSet[T_Parameter]:
        output = type(self)(self.data.difference(other.data))
        self_any = self.subset_any_type()
        other_any = other.subset_any_type()
        if not self_any and not other_any:
            # Operation done using hashes
            return output
        # Operation done using equality as ANY_STR and ANY_INT does not match on hash
        # Note that this a Parameter with ANY_STR in the path can remove multiple items.
        discard = set()
        for item in output:
            for any_ in other_any:
                if item == any_:
                    discard.add(item)
            for any_ in self_any:
                if item == any_:
                    discard.add(item)
        for d in discard:
            output.discard(d)
        return output

    def __sub__(self, other: AbstractSet) -> ParameterSet[T_Parameter]:
        return self.difference(type(self)(other))


class ParameterSpecSet(ParameterSet[ParameterSpec]):
    def __init__(self, iterable: Iterable[ParameterSpec] = ()) -> None:
        super().__init__(iterable)
        self.is_complete = True

    @property
    def required(self) -> ParameterSet[ParameterSpec]:
        return ParameterSet[ParameterSpec](parameter for parameter in self if parameter.is_required)

    def as_camel_case(self) -> ParameterSpecSet:
        output = ParameterSpecSet(
            ParameterSpec(
                tuple(to_camel_case(name) if name not in SINGLETONS else name for name in parameter.path),
                parameter.types,
                parameter.is_required,
                parameter._is_nullable,
            )
            for parameter in self
        )
        output.is_complete = self.is_complete
        return output

    def update(self, other: ParameterSet[ParameterSpec]) -> None:
        if isinstance(other, ParameterSpecSet):
            self.is_complete &= other.is_complete
        super().update(other)
