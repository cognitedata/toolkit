import sys
from functools import cached_property
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.utils import iterate_modules
from cognite_toolkit._cdf_tk.utils.modules import parse_user_selected_modules

from ._module_toml import ModuleToml

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Resource(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    path: Path

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls(path=path)


class Module(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    path: Path
    resources: list[Resource]
    definition: ModuleToml | None = None

    @classmethod
    def load(cls, path: Path, resource_paths: list[Path]) -> Self:
        definition = ModuleToml.load(path / ModuleToml.filename) if (path / ModuleToml.filename).exists() else None
        resources = [Resource.load(path=resource_path) for resource_path in resource_paths]
        return cls(path=path, resources=resources, definition=definition)


class ModulesDirectory(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    modules: list[Module] = Field(default_factory=list)

    @classmethod
    def load(cls, organization_dir: Path, selection: list[str | Path] | None = None) -> Self:
        selected = parse_user_selected_modules(selection, organization_dir) if selection else None
        return cls(
            modules=[
                Module.load(path=module_path, resource_paths=resource_paths)
                for module_path, resource_paths in iterate_modules(organization_dir / MODULES)
                if cls._is_selected(module_path, organization_dir, selected)
            ],
        )

    @staticmethod
    def _is_selected(module_path: Path, organization_dir: Path, selection: list[str | Path] | None) -> bool:
        if selection is None:
            return True
        relative = module_path.relative_to(organization_dir)
        return module_path.name in selection or relative in selection or any(p in selection for p in relative.parents)

    @cached_property
    def paths(self) -> list[Path]:
        return [module.path for module in self.modules]

    @cached_property
    def available_paths(self) -> set[Path]:
        return {module.path for module in self.modules}
