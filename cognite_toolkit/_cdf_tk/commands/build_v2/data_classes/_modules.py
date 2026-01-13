import sys
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import EXCL_FILES, MODULES
from cognite_toolkit._cdf_tk.cruds import ALL_CRUDS_BY_FOLDER_NAME, CRUDS_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.data_classes import IssueList
from cognite_toolkit._cdf_tk.data_classes._issues import (
    ModuleLoadingIssue,
)
from cognite_toolkit._cdf_tk.data_classes._module_toml import ModuleToml
from cognite_toolkit._cdf_tk.utils.modules import module_path, parse_user_selected_modules

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

        detected_modules: defaultdict[Path, set[Path]] = defaultdict(set)
        unrecognized_resources: defaultdict[Path, set[str]] = defaultdict(set)
        unselected_modules: set[Path] = set()

        # iterate over all resource files in the modules root directory,
        # using resources' parent hierarchy to determine the module folders

        for resource_file in [p for p in modules_root.glob("**/*.y*ml") if p.name not in EXCL_FILES]:
            # Get the module folder for the resource file.
            module_candidate = cls.get_module_folder(resource_file)
            if module_candidate is None:
                continue

            # If the module has already been processed, skip it.
            if module_candidate in detected_modules:
                continue

            # Skip the modules that do not match the selection
            if not cls._matches_selection(module_candidate, modules_root, selected):
                unselected_modules.add(module_candidate)
                continue

            # iterate over the resource folders in the module.
            for resource_folder in [d for d in module_candidate.iterdir() if d.is_dir()]:
                if resource_folder.name not in CRUDS_BY_FOLDER_NAME:
                    unrecognized_resources[module_candidate].add(resource_folder.name)
                    continue

                detected_modules[module_candidate].add(resource_folder)

                # If the current module is a submodule of another module, remove the parent module.
                # We only keep the deepest module.
                parent_modules_to_remove = [k for k in detected_modules if k in module_candidate.parents]
                for k in parent_modules_to_remove:
                    issues.append(
                        ModuleLoadingIssue(
                            message=f"Module {module_path(organization_dir, k)!r} is skipped because it has submodules"
                        )
                    )
                    unselected_modules.add(k)
                    detected_modules.pop(k)

                    # If the submodule was mistakenly marked as an unrecognized resource folder to the parent,
                    # remove it from the list of unrecognized resources.
                    if k in unrecognized_resources and module_candidate.name in unrecognized_resources[k]:
                        unrecognized_resources[k].discard(module_candidate.name)
                        if not unrecognized_resources[k]:  # Remove empty sets
                            unrecognized_resources.pop(k)

        loaded_modules: list[Module] = []
        for module_candidate, resource_folders in detected_modules.items():
            loaded_modules.append(
                Module.load(
                    path=module_candidate, resource_paths=[module_candidate / folder for folder in resource_folders]
                )
            )

        for k, v in unrecognized_resources.items():
            issues.append(
                ModuleLoadingIssue(
                    message=f"Module {module_path(organization_dir, k)!r} contains unrecognized resource folder(s): {', '.join(v)}"
                )
            )
        # for k, v in detected_disabled_resource_folders.items():
        #     issues.append(
        #         ModuleLoadingIssue(
        #             message=f"Module {module_path(organization_dir, k)!r} contains unsupported resource folder(s), check flags in cdf.toml: {', '.join(v)}"
        #         )
        #     )

        return cls(
            organization_dir=organization_dir,
            modules=loaded_modules,
        ), issues

    @classmethod
    def get_module_folder(cls, resource_file: Path) -> Path | None:
        # recognize the module by containing a resource accosiated by a CRUD.
        # Special case: if the resource folder is a subfolder of a CRUD, return the parent of the subfolder.
        resource_folder = resource_file.parent
        crud = next(iter(ALL_CRUDS_BY_FOLDER_NAME.get(resource_folder.name, [])), None)
        if crud:
            # iterate over the parents of the resource folder until we find the module folder. This is to handle the special case of a subfolder of a CRUD, or yamls in for example function subfolders.
            for p in resource_file.parents:
                if p.name == crud.folder_name:
                    return p.parent
                if p.name == MODULES:
                    return p
        return None

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
