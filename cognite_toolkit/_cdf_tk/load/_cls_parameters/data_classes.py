from __future__ import annotations

import typing
from collections.abc import Hashable, Iterable, MutableSet
from dataclasses import dataclass
from functools import total_ordering
from typing import Generic, TypeVar


@total_ordering
@dataclass(frozen=True)
class Parameter:
    path: tuple[str | int, ...]
    type: str

    def __lt__(self, other: Parameter) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path < other.path

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Parameter):
            return NotImplemented
        return self.path == other.path and self.type == other.type


@dataclass(frozen=True)
class ParameterSpec(Parameter):
    is_required: bool
    _is_nullable: bool | None = None

    @property
    def is_nullable(self) -> bool:
        return self._is_nullable or not self.is_required


@dataclass(frozen=True)
class ParameterValue(Parameter):
    value: str | int | float | bool | list | dict


T_Parameter = TypeVar("T_Parameter", bound=Parameter)


class ParameterSet(Hashable, MutableSet, Generic[T_Parameter]):
    def __init__(self, iterable: Iterable[T_Parameter] = ()) -> None:
        self.data: set[T_Parameter] = set(iterable)

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


class ParameterSpecSet(ParameterSet[ParameterSpec]):
    def __init__(self, iterable: Iterable[ParameterSpec] = ()) -> None:
        super().__init__(iterable)
        self.is_complete = True

    @property
    def required(self) -> ParameterSet[ParameterSpec]:
        return ParameterSet[ParameterSpec](parameter for parameter in self if parameter.is_required)

    def update(self, other: ParameterSet[ParameterSpec]) -> None:
        if isinstance(other, ParameterSpecSet):
            self.is_complete &= other.is_complete
        super().update(other)
