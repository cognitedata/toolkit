from __future__ import annotations

from collections.abc import MutableSequence
from dataclasses import dataclass
from pathlib import Path
from typing import Generic

from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID


@dataclass
class Location:
    path: Path
    hash: str


@dataclass
class ResourceInfo(Generic[T_ID]):
    identifier: T_ID
    location: Location
    kind: str


class ResourceList(list, MutableSequence[ResourceInfo]): ...


@dataclass
class ModuleInfo:
    name: str
    location: Location
    build_variables: dict[str, str | int | float | bool | None]
    resources: dict[str, ResourceList]


@dataclass
class ModuleList(list, MutableSequence[ModuleInfo]): ...


@dataclass
class ModulesInfo:
    version: str
    modules: ModuleList


@dataclass
class BuildInfo:
    modules: ModulesInfo
