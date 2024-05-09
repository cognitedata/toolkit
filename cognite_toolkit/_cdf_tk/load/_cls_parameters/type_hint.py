from __future__ import annotations

import inspect
import types
import typing
from typing import Any, ClassVar, get_origin


class _TypeHint:
    _BASE_TYPES: ClassVar[set[str]] = {t.__name__ for t in (str, int, float, bool)}
    _CONTAINER_TYPES: ClassVar[set[str]] = {t.__name__ for t in (list, dict)}

    def __init__(self, raw: Any) -> None:
        self.raw = raw
        self._container_type = get_origin(raw)
        self.args = typing.get_args(raw)
        self._is_nullable = False
        if self._is_union(self._container_type) and self.args:
            inner_container = typing.get_origin(self.args[0])
            if inner_container:
                self._is_nullable = any(arg in [None, types.NoneType] for arg in self.args)
                self._container_type = inner_container
                self.args = typing.get_args(self.args[0])

    def __str__(self) -> str:
        if self._container_type and self._container_type not in [types.UnionType, typing.Union]:
            value = self._container_type.__name__
        else:
            value = self.arg.__name__
        if value in self._CONTAINER_TYPES or value in self._BASE_TYPES:
            return value
        elif value == "Literal":
            return "str"
        return "dict"

    @classmethod
    def _is_union(cls, x: Any) -> bool:
        return x in [types.UnionType, typing.Union]

    @property
    def arg(self) -> Any:
        if self._is_union(self._container_type) and self.args:
            value = self.args[0]
        else:
            value = self.raw
        if self._is_union(value):
            value = value.__args__[0]
        return value

    @property
    def is_base_type(self) -> bool:
        return str(self) in self._BASE_TYPES

    @property
    def is_nullable(self) -> bool:
        return self._is_nullable or any(arg is None or arg is types.NoneType for arg in self.args)

    @property
    def is_class(self) -> bool:
        return inspect.isclass(self.arg)

    @property
    def is_dict_type(self) -> bool:
        return self._container_type is dict

    @property
    def is_list_type(self) -> bool:
        return self._container_type is list

    def __repr__(self) -> str:
        return repr(self.arg)
