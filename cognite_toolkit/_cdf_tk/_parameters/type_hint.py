from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Iterable
from typing import Any, get_origin

from .constants import BASE_TYPES, TYPES


class TypeHint:
    def __init__(self, raw: Any) -> None:
        self.raw = raw
        self._container_type = get_origin(raw)
        self.is_union = self._is_union(self._container_type)
        if self.is_union:
            self.args = typing.get_args(raw)
        else:
            self.args = (raw,)

    def __str__(self) -> str:
        return ",".join(self.types)

    @classmethod
    def _is_union(cls, x: Any) -> bool:
        try:
            return x in [types.UnionType, typing.Union]  # type: ignore[attr-defined]
        except AttributeError:
            # Python 3.9
            return x is typing.Union

    @classmethod
    def _is_none_type(cls, x: Any) -> bool:
        try:
            return x in [None, types.NoneType]  # type: ignore[attr-defined]
        except AttributeError:
            # Python 3.9
            return x is None

    @classmethod
    def _as_str(cls, arg: Any) -> str:
        try:
            value = arg.__name__
        except AttributeError:
            # Python 3.9
            value = str(arg).removeprefix("typing.")
            if "[" in value:
                value = value.split("[")[0]
        if value in TYPES:
            return value
        elif value == "Literal":
            return "str"
        return "dict"

    @property
    def types(self) -> list[str]:
        return [self._as_str(arg) for arg in self.args if not self._is_none_type(arg)]

    @property
    def frozen_types(self) -> frozenset[str]:
        return frozenset(self.types)

    @property
    def is_base_type(self) -> bool:
        return any(type_ in BASE_TYPES for type_ in self.types)

    @property
    def is_nullable(self) -> bool:
        return any(self._is_none_type(arg) for arg in self.args)

    @property
    def is_class(self) -> bool:
        if self.is_union or self.is_dict_type or self.is_list_type:
            return False
        return inspect.isclass(self.args[0])

    @property
    def _get_origins(self) -> tuple[Any, ...]:
        return tuple(get_origin(arg) for arg in self.args)

    @property
    def is_dict_type(self) -> bool:
        return any(arg is dict for arg in self._get_origins)

    @property
    def is_list_type(self) -> bool:
        return any(arg is list for arg in self._get_origins)

    @property
    def container_args(self) -> tuple[Any, ...]:
        return typing.get_args(self.args[0])

    def __repr__(self) -> str:
        return repr(self.raw)

    @property
    def sub_hints(self) -> Iterable[TypeHint]:
        for arg in self.args:
            if self._is_none_type(arg):
                continue
            yield TypeHint(arg)


class _AnyInt(int):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return True
        return NotImplemented

    def __hash__(self) -> int:
        return id(ANY_INT)

    def __str__(self) -> str:
        return "AnyInt"

    def __repr__(self) -> str:
        return "AnyInt"


class _AnyStr(str):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return True
        return NotImplemented

    def __hash__(self) -> int:
        return id(ANY_STR)

    def __str__(self) -> str:
        return "AnyStr"

    def __repr__(self) -> str:
        return "AnyStr"


ANY_INT = _AnyInt()
ANY_STR = _AnyStr()
