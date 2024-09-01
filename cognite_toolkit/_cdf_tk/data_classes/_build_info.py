from __future__ import annotations

from collections.abc import MutableSequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Generic

from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID

from ._base import ConfigCore


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

    @classmethod
    def load(cls, data: dict[str, Any]) -> ModulesInfo:
        raise NotImplementedError


@dataclass
class BuildInfo(ConfigCore):
    filename: ClassVar[str] = "build_info.{build_env}.yaml"
    modules: ModulesInfo

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> BuildInfo:
        return cls(filepath, ModulesInfo.load(data))


class ModuleResources:
    """This class is used to retrieve resource information from the build info.

    It is responsible for ensuring that the build info is up-to-date with the
    latest changes in the source directory.
    """

    def __init__(self, project_dir: Path, build_env: str) -> None:
        self._build_info = BuildInfo.load_from_directory(project_dir, build_env)

    def list(self) -> ModuleList:
        raise NotImplementedError

    def retrieve_resource(self, kind: str, identifier: T_ID) -> ResourceInfo[T_ID]:
        raise NotImplementedError

    def retrieve_resources(self, kind: str) -> ResourceList:
        raise NotImplementedError
