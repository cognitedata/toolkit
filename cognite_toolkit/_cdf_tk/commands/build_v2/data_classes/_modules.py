import os
import sys
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, EXCLUDED_CRUDS
from cognite_toolkit._cdf_tk.data_classes import IssueList
from cognite_toolkit._cdf_tk.data_classes._issues import (
    ModuleLoadingDisabledResourceIssue,
    ModuleLoadingIssue,
    ModuleLoadingUnrecognizedResourceIssue,
)
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.utils.modules import parse_user_selected_modules

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Module(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    path: Path
    definition: ModuleToml | None = None

    @classmethod
    def load(cls, path: Path, resource_paths: list[Path]) -> Self:
        definition = ModuleToml.load(path / ModuleToml.filename) if (path / ModuleToml.filename).exists() else None
        return cls(path=path, definition=definition)


class Modules(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    organization_dir: Path
    modules: list[Module] = Field(default_factory=list)

    @classmethod
    def load(cls, organization_dir: Path, selection: list[str | Path] | None = None) -> tuple[Self, IssueList]:
        if selection is None:
            # Treat "no selection" as selecting the whole modules tree.
            # This makes the implicit default equivalent to selecting `MODULES`.
            selected: list[str | Path] | None = [MODULES]
        else:
            selected = parse_user_selected_modules(selection, organization_dir)

        modules_root = organization_dir / MODULES
        issues = IssueList()

        if not modules_root.exists():
            issues.append(
                ModuleLoadingIssue(
                    path=modules_root,
                )
            )
            return cls(organization_dir=organization_dir, modules=[]), issues

        # Walk modules_root: all leaf directories are resource presumed to be resource folders
        # Their parents are module candidates
        excluded_folder_names = {crud.folder_name for crud in EXCLUDED_CRUDS}

        # Map module paths to their resource folders and issues
        module_candidates: defaultdict[Path, set[str]] = defaultdict(set)
        unrecognized_resource_folders: defaultdict[Path, set[str]] = defaultdict(set)
        disabled_resource_folders: defaultdict[Path, set[str]] = defaultdict(set)

        for dirpath, dirnames, filenames in os.walk(modules_root):
            current_dir = Path(dirpath)
            module_candidate = current_dir.parent

            if not cls._matches_selection(module_candidate, modules_root, selected):
                continue

            # if (
            #     dirnames # directories with subdirectories
            #     or not filenames # directories with no files
            #     or not cls._matches_selection(module_candidate, modules_root, selected) # not selected
            # ):
            #     continue
            # if dirnames:
            #     continue
            # if not filenames:
            #     continue
            # if not cls._matches_selection(module_candidate, modules_root, selected):
            #     continue

            if current_dir.name in excluded_folder_names:
                disabled_resource_folders[module_candidate].add(current_dir.name)
            elif current_dir.name not in CRUDS_BY_FOLDER_NAME:
                unrecognized_resource_folders[module_candidate].add(current_dir.name)
            else:
                module_candidates[module_candidate].add(current_dir.name)

        loaded_modules = []
        for module_candidate, resource_folders in module_candidates.items():
            loaded_modules.append(
                Module.load(
                    path=module_candidate, resource_paths=[module_candidate / folder for folder in resource_folders]
                )
            )

        for k, v in unrecognized_resource_folders.items():
            issues.append(
                ModuleLoadingUnrecognizedResourceIssue(
                    path=k,
                    unrecognized_resource_folders=list(v),
                )
            )
        for k, v in disabled_resource_folders.items():
            issues.append(
                ModuleLoadingDisabledResourceIssue(
                    path=k,
                    disabled_resource_folders=list(v),
                )
            )

        return cls(
            organization_dir=organization_dir,
            modules=loaded_modules,
        ), issues

    @staticmethod
    def _matches_selection(module_candidate: Path, modules_root: Path, selected: list[str | Path] | None) -> bool:
        if not selected:
            return True

        rel = module_candidate.relative_to(modules_root)
        rel_parts = [p.lower() for p in rel.parts]
        if not rel_parts:
            # module_candidate is the modules_root itself
            return False
        name_lower = rel_parts[-1]
        modules_lower = MODULES.lower()

        for sel in selected:
            sel_path = Path(sel) if isinstance(sel, str) else sel

            sel_parts = [p.lower() for p in sel_path.parts]
            if not sel_parts:
                continue

            if sel_parts[0] == modules_lower:
                sel_parts = sel_parts[1:]

            if not sel_parts:
                return True

            if len(sel_parts) == 1 and name_lower == sel_parts[0]:
                return True

            if rel_parts[: len(sel_parts)] == sel_parts:
                return True

        return False

    @classmethod
    def _is_selected(cls, module_path: Path, organization_dir: Path, selected: list[str | Path] | None) -> bool:
        if selected is None:
            return True

        module_name = module_path.name
        try:
            rel_to_org = module_path.relative_to(organization_dir)
        except ValueError:
            rel_to_org = module_path
        try:
            rel_to_modules = module_path.relative_to(organization_dir / MODULES)
        except ValueError:
            rel_to_modules = module_path

        for sel in selected:
            if isinstance(sel, str):
                if sel == module_name:
                    return True
                continue

            # Path selections can be absolute or relative (to org or modules root).
            if sel == module_path or sel == rel_to_org or sel == rel_to_modules:
                return True
            if sel in rel_to_org.parents or sel in rel_to_modules.parents:
                return True

        return False
