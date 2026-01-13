import sys
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, EXCLUDED_CRUDS
from cognite_toolkit._cdf_tk.data_classes import IssueList
from cognite_toolkit._cdf_tk.data_classes._issues import (
    ModuleLoadingIssue,
)
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.utils.modules import is_module_path, module_path, parse_user_selected_modules

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
            issues.append(ModuleLoadingIssue(message="Module root directory 'modules' not found"))
            return cls(organization_dir=organization_dir, modules=[]), issues

        # Map module paths to their resource folders and issues
        detected_modules: defaultdict[Path, set[str]] = defaultdict(set)
        detected_unrecognized_resource_folders: defaultdict[Path, set[str]] = defaultdict(set)
        detected_disabled_resource_folders: defaultdict[Path, set[str]] = defaultdict(set)
        detected_unselected_modules: set[Path] = set()

        for current_dir in modules_root.glob("**/"):
            # Skip the modules root directory itself
            if current_dir == modules_root:
                continue

            # Skip the modules that have already been detected
            module_candidate = current_dir.parent
            if module_candidate in detected_modules:
                continue

            # Skip the modules that do not match the selection
            if not cls._matches_selection(module_candidate, modules_root, selected):
                detected_unselected_modules.add(module_candidate)
                continue

            # A module is recognized by having one or more resource folders.
            # We also consider resource folders that are not enabled by flags because we
            # want to give the user a hint to check the flags in cdf.toml.
            module_subfolders = [d for d in module_candidate.iterdir() if d.is_dir()]
            excluded_folder_names = {crud.folder_name for crud in EXCLUDED_CRUDS}
            resource_folders = {d.name for d in module_subfolders if d.name in CRUDS_BY_FOLDER_NAME.keys()}
            disabled_resource_folders = {d.name for d in module_subfolders if d.name in excluded_folder_names}
            unrecognized_resource_folders = {
                d.name
                for d in module_subfolders
                if d.name not in CRUDS_BY_FOLDER_NAME
                and d.name not in excluded_folder_names
                and not is_module_path(d)  # Exclude submodules
            }

            if resource_folders:
                detected_modules[module_candidate] = resource_folders

                # If the current module is a submodule of another module, remove the parent module from the detected modules. We only keep the deepest module.
                for k, v in detected_modules.items():
                    if k in module_candidate.parents:
                        issues.append(
                            ModuleLoadingIssue(
                                message=f"Module {module_path(organization_dir, k)!r} is skipped because it has submodules"
                            )
                        )
                        detected_unselected_modules.add(k)
                        detected_modules.pop(k)
                        break

                # if the submodule was mistakenly marked as an unrecognized resource folder
                # to the parent, remove it from the list
                if module_candidate.name in unrecognized_resource_folders:
                    unrecognized_resource_folders.discard(module_candidate.name)
            else:
                # Skip the modules that have no resource folders.
                # Note: if it only has disabled resource folders, we will skip it unnoticed.
                continue
            if disabled_resource_folders:
                detected_disabled_resource_folders[module_candidate] = disabled_resource_folders
            if unrecognized_resource_folders:
                detected_unrecognized_resource_folders[module_candidate] = unrecognized_resource_folders

        loaded_modules = []
        for module_candidate, resource_folders in detected_modules.items():
            loaded_modules.append(
                Module.load(
                    path=module_candidate, resource_paths=[module_candidate / folder for folder in resource_folders]
                )
            )

        for k, v in detected_unrecognized_resource_folders.items():
            issues.append(
                ModuleLoadingIssue(
                    message=f"Module {module_path(organization_dir, k)!r} contains unrecognized resource folder(s): {', '.join(v)}"
                )
            )
        for k, v in detected_disabled_resource_folders.items():
            issues.append(
                ModuleLoadingIssue(
                    message=f"Module {module_path(organization_dir, k)!r} contains unsupported resource folder(s), check flags in cdf.toml: {', '.join(v)}"
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
