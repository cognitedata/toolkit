from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Iterable
from typing import Any, ClassVar, get_origin


class TypeHint:
    _BASE_TYPES: ClassVar[set[str]] = {t.__name__ for t in (str, int, float, bool)}
    _CONTAINER_TYPES: ClassVar[set[str]] = {t.__name__ for t in (list, dict)}
    _TYPES: ClassVar[set[str]] = _BASE_TYPES | _CONTAINER_TYPES

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
        return x in [types.UnionType, typing.Union]

    @classmethod
    def _is_none_type(cls, x: Any) -> bool:
        return x in [None, types.NoneType]

    @classmethod
    def _as_str(cls, arg: Any) -> str:
        value = arg.__name__
        if value in cls._TYPES:
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
        return any(type_ in self._BASE_TYPES for type_ in self.types)

    @property
    def is_nullable(self) -> bool:
        return any(self._is_none_type(arg) for arg in self.args)

    @property
    def is_class(self) -> bool:
        if self.is_union:
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
