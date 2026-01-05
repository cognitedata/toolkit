import sys
from collections.abc import Iterator
from functools import cached_property
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes._issues import IssueList, ModuleDirectoryIssue
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


class ModuleRootDirectory(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
    )

    organization_dir: Path
    modules: list[Module] = Field(default_factory=list)

    @classmethod
    def load(cls, organization_dir: Path, selection: list[str | Path] | None = None) -> Self:
        # selection semantics:
        # - selection is None  -> select all modules
        # - selection is []    -> select none (explicit "no modules selected")
        selected = None if selection is None else parse_user_selected_modules(selection, organization_dir)
        return cls(
            organization_dir=organization_dir,
            modules=[
                Module.load(path=module_path, resource_paths=resource_paths)
                for module_path, resource_paths in iterate_modules(organization_dir / MODULES)
                if cls._is_selected(module_path, organization_dir, selected)
            ],
        )

    def verify_integrity(self) -> IssueList:
        issues = IssueList()
        if not self.organization_dir.exists() or not self.organization_dir.is_dir():
            issues.append(
                ModuleDirectoryIssue(
                    message=f"Organization directory {self.organization_dir.as_posix()!r} is not a directory."
                )
            )
        elif not self.organization_dir.joinpath(MODULES).is_dir():
            issues.append(
                ModuleDirectoryIssue(
                    message=f"Modules directory {self.organization_dir.joinpath(MODULES).as_posix()!r} is not a directory."
                )
            )
        return issues

    @staticmethod
    def _is_selected(module_path: Path, organization_dir: Path, selection: list[str | Path] | None) -> bool:
        if selection is None:
            return True
        relative = module_path.relative_to(organization_dir)
        return module_path.name in selection or relative in selection or any(p in selection for p in relative.parents)

    def __iter__(self) -> Iterator[Module]:  # type: ignore[override]
        return iter(self.modules)

    def __len__(self) -> int:
        # Enables correct truthiness checks (e.g. `if not modules.selected:`)
        return len(self.modules)

    @cached_property
    def available(self) -> set[str | Path]:
        """Ways of selecting the loaded modules (name and relative paths)."""
        selections: set[str | Path] = set()
        for module in self.modules:
            relative = module.path.relative_to(self.organization_dir)
            selections.add(module.path.name)
            selections.add(relative)
            selections.update(relative.parents)
        return selections

    @cached_property
    def available_names(self) -> set[str]:
        return {item for item in self.available if isinstance(item, str)}

    @cached_property
    def available_paths(self) -> set[Path]:
        return {item for item in self.available if isinstance(item, Path)}

    @property
    def selected(self) -> "ModuleRootDirectory":
        # ModuleRootDirectory currently only loads selected modules
        return self

    def as_path_by_name(self) -> dict[str, list[Path]]:
        module_path_by_name: dict[str, list[Path]] = {}
        for module in self.modules:
            module_path_by_name.setdefault(module.path.name, []).append(module.path.relative_to(self.organization_dir))
        return module_path_by_name

    @cached_property
    def paths(self) -> list[Path]:
        return [module.path for module in self.modules]
