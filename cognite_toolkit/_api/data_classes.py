from __future__ import annotations

from collections import UserDict, UserList
from dataclasses import dataclass

__all__ = ["Variable", "Variables", "Module", "ModuleList"]

from pathlib import Path

from cognite_toolkit._cdf_tk.load import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.templates.data_classes import ConfigEntry, Environment, InitConfigYAML

_NOT_SET = object()
_DUMMY_ENVIRONMENT = Environment(
    name="not used",
    project="not used",
    build_type="not used",
    selected_modules_and_packages=[],
    common_function_code="",
)


@dataclass
class Variable:
    name: str
    value: str
    default: str
    description: str | None = None


@dataclass
class Variables(UserDict):

    @classmethod
    def _load(cls, module_path: Path, default_variables: dict[tuple[str, ...], ConfigEntry]) -> Variables:
        _ = dict(InitConfigYAML(_DUMMY_ENVIRONMENT).load_variables(module_path))

        raise NotImplementedError


@dataclass(frozen=True)
class Module:
    name: str
    variables: Variables
    resource_types: tuple[str, ...]
    packages: frozenset[str]
    _source: Path
    _readme: str | None = None

    @classmethod
    def _load(
        cls, module_path: Path, packages: frozenset[str], default_variables: dict[tuple[str, ...], ConfigEntry]
    ) -> Module:
        readme: str | None = None
        if (readme_path := module_path / "README.md").exists():
            readme = readme_path.read_text()
        resource_types = tuple(
            resource_path.name for resource_path in module_path.iterdir() if resource_path.name in LOADER_BY_FOLDER_NAME
        )

        return cls(
            name=module_path.name,
            variables=Variables._load(module_path, default_variables),
            resource_types=resource_types,
            packages=packages,
            _source=module_path,
            _readme=readme,
        )


class ModuleList(UserList):
    @property
    def names(self) -> list[str]:
        return [module.name for module in self.data]

    def __getitem__(self, item: str) -> Module:  # type: ignore[override]
        for module in self.data:
            if module.name == item:
                return module
        raise KeyError(f"Module {item!r} not found")
