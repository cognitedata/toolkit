from __future__ import annotations

import enum
import inspect
import itertools
import types
import typing
from collections.abc import Iterable, Mapping, MutableMapping, MutableSequence, Sequence
from typing import Any, get_origin

from .constants import BASE_TYPES, TYPES


class TypeHint:
    _DICT_TYPES = {dict, typing.Dict, MutableSequence, MutableMapping, typing.Mapping, Mapping}  # noqa UP006
    _LIST_TYPES = {list, typing.Sequence, Sequence, typing.List, MutableSequence}  # noqa UP006

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
        if value.casefold() in TYPES:
            return value.casefold()
        elif value == "Literal":
            return "str"
        elif value == "Sequence":
            return "list"
        elif value == "Any":
            return "unknown"
        elif inspect.isclass(arg) and issubclass(arg, enum.Enum):
            return "str"
        return "dict"

    @property
    def types(self) -> list[str]:
        return list({self._as_str(arg) for arg in self.args if not self._is_none_type(arg)})

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
    def is_any(self) -> bool:
        return any(arg is typing.Any for arg in self.args)

    @property
    def is_user_defined_class(self) -> bool:
        return any(
            inspect.isclass(arg) and arg.__module__ not in {"typing", "builtins", "collections.abc"}
            for arg in self.args
        )

    @property
    def _get_origins(self) -> tuple[Any, ...]:
        return tuple(get_origin(arg) for arg in self.args)

    @property
    def is_dict_type(self) -> bool:
        return any(arg in self._DICT_TYPES for arg in itertools.chain(self._get_origins, self.args))

    @property
    def is_list_type(self) -> bool:
        return any(arg in self._LIST_TYPES for arg in itertools.chain(self._get_origins, self.args))

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
