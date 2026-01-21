from pathlib import Path

from cognite_toolkit._cdf_tk.constants import EXCL_FILES, MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA
from cognite_toolkit._cdf_tk.data_classes import IssueList
from cognite_toolkit._cdf_tk.data_classes._issues import ModuleLoadingIssue
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.utils import humanize_collection, module_path_display_name


class ModulesParser:
    def __init__(self, organization_dir: Path, selected: list[str | Path] | None = None):
        self.organization_dir = organization_dir
        self.selected = selected

    def parse(self) -> tuple[list[Path], IssueList]:
        modules_root = self.organization_dir / MODULES
        if not modules_root.exists():
            raise ToolkitError(f"Module root directory '{modules_root}' not found")

        module_paths: list[Path] = []
        excluded_module_paths: list[Path] = []
        issues: IssueList = IssueList()
        for resource_file in self.organization_dir.glob("**/*.y*ml"):
            if resource_file.name in EXCL_FILES:
                continue

            # Get the module folder for the resource file.
            module_path = self._get_module_path_from_resource_file_path(resource_file)
            if not module_path:
                continue

            # If the module has already been processed, skip it.
            if module_path in module_paths or module_path in excluded_module_paths:
                continue

            # Skip the modules that do not match the selection
            if not self._matches_selection(module_path, modules_root, self.selected):
                excluded_module_paths.append(module_path)
                continue

            module_paths.append(module_path)

        deepest_module_paths = self._find_modules_with_submodules(module_paths)
        parent_module_paths = set(module_paths) - set(deepest_module_paths)
        if parent_module_paths:
            module_paths = deepest_module_paths
            issues.extend(
                ModuleLoadingIssue(
                    message=f"Module {module_path_display_name(self.organization_dir, parent_module_path)!r} is skipped because it has submodules"
                )
                for parent_module_path in parent_module_paths
            )

        valid_module_paths: list[Path] = []
        for module_path in module_paths:
            valid_module_path, issue = self._check_resource_folder_content(module_path)
            if issue:
                issues.append(issue)
            if valid_module_path:
                valid_module_paths.append(valid_module_path)

        return valid_module_paths, issues

    def _get_module_path_from_resource_file_path(self, resource_file: Path) -> Path | None:
        # recognize the module by containing a resource associated by a CRUD.
        # Special case: if the resource folder is a subfolder of a CRUD, return the parent of the subfolder.
        resource_folder = resource_file.parent
        crud = next(iter(CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA.get(resource_folder.name, [])), None)
        if crud:
            # iterate over the parents of the resource folder until we find the module folder.
            # This is to handle the special case of a subfolder of a CRUD, or yamls in for example function subfolders.
            for p in resource_file.parents:
                if p.name == crud.folder_name:
                    return p.parent
                if p.name == MODULES:
                    return p
        return None

    def _check_resource_folder_content(self, module_path: Path) -> tuple[None | Path, ModuleLoadingIssue | None]:
        resource_folder_names = {d.name for d in module_path.iterdir() if d.is_dir()}
        unrecognized_resource_folder_names = resource_folder_names - CRUDS_BY_FOLDER_NAME.keys()

        issue = (
            ModuleLoadingIssue(
                message=f"Module {module_path_display_name(self.organization_dir, module_path)!r} contains unrecognized resource folder(s): {humanize_collection(unrecognized_resource_folder_names)}"
            )
            if unrecognized_resource_folder_names
            else None
        )

        has_valid_resource_folders = bool(resource_folder_names & CRUDS_BY_FOLDER_NAME.keys())
        return (module_path if has_valid_resource_folders else None, issue)

    def _matches_selection(self, module_path: Path, modules_root: Path, selected: list[str | Path] | None) -> bool:
        if not selected:
            return True

        rel = module_path.relative_to(modules_root)
        rel_parts = [p.lower() for p in rel.parts]
        if not rel_parts:
            # module_path is the modules_root itself
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

    def _find_modules_with_submodules(self, module_paths: list[Path]) -> list[Path]:
        """Remove parent modules when they have submodules. Keep only the deepest modules."""
        return [
            module_path
            for module_path in module_paths
            if not any(
                module_path in other_module_path.parents
                for other_module_path in module_paths
                if other_module_path != module_path
            )
        ]
