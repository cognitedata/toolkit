from __future__ import annotations

from collections import UserDict, UserList
from dataclasses import dataclass

__all__ = ["NOT_SET", "Variable", "Variables", "Module", "ModuleList"]

NOT_SET = object()


@dataclass
class Variable:
    name: str
    value: str
    default: str
    description: str | None = None


@dataclass
class Variables(UserDict): ...


@dataclass
class Module:
    name: str
    variables: Variables
    source: str
    resource_types: tuple[str, ...]
    packages: frozenset[str]
    _readme: str | None = None


class ModuleList(UserList):
    @property
    def names(self) -> list[str]:
        return [module.name for module in self.data]
