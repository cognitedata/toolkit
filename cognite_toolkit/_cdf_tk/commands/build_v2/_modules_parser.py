from pathlib import Path

from cognite_toolkit._cdf_tk.constants import EXCL_FILES, MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA
from cognite_toolkit._cdf_tk.data_classes import IssueList
from cognite_toolkit._cdf_tk.data_classes._issues import ModuleLoadingIssue
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.utils import module_path_display_name


class ModulesParser:
    def __init__(self, organization_dir: Path, selected: list[str | Path] | None = None):
        self.organization_dir = organization_dir
        self.selected = selected
        self.issues = IssueList()

    def parse(self) -> list[Path]:
        modules_root = self.organization_dir / MODULES
        if not modules_root.exists():
            raise ToolkitError(f"Module root directory '{modules_root.as_posix()}' not found")

        module_paths: list[Path] = []
        for resource_file in self.organization_dir.glob("**/*.y*ml"):
            if resource_file.name in EXCL_FILES:
                continue

            # Get the module folder for the resource file.
            module_path = self._get_module_path_from_resource_file_path(resource_file)
            if not module_path:
                continue

            # If the module has already been processed, skip it.
            if module_path in module_paths:
                continue

            module_paths.append(module_path)

        deepest_module_paths = self._find_modules_with_submodules(module_paths)
        parent_module_paths = set(module_paths) - set(deepest_module_paths)
        if parent_module_paths:
            module_paths = deepest_module_paths
            self.issues.extend(
                ModuleLoadingIssue(
                    message=f"Module {module_path_display_name(self.organization_dir, parent_module_path)!r} is skipped because it has submodules",
                )
                for parent_module_path in parent_module_paths
            )

        all_module_paths = module_paths

        if self.selected:
            normalized_selected = self._normalize_selection(self.selected)
            selected_module_paths = [
                module_path
                for module_path in all_module_paths
                if self._matches_selection(module_path, modules_root, normalized_selected)
            ]
            for selected_module in self.selected:
                normalized_selected_item = self._normalize_selection([selected_module])
                has_match = any(
                    self._matches_selection(found_module, modules_root, normalized_selected_item)
                    for found_module in all_module_paths
                )
                if not has_match:
                    self.issues.append(
                        ModuleLoadingIssue(
                            message=f"Module '{selected_module}' not found",
                        )
                    )

            module_paths = selected_module_paths

        valid_module_paths: list[Path] = []
        for module_path in module_paths:
            valid_module_path, issue = self._check_resource_folder_content(module_path)
            if issue:
                self.issues.append(issue)
            if valid_module_path:
                valid_module_paths.append(valid_module_path)

        return valid_module_paths

    def _get_module_path_from_resource_file_path(self, resource_file: Path) -> Path | None:
        # recognize the module by traversing the parents of the resource file until we find a CRUD folder

        for parent in resource_file.parents:
            if parent.name in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA:
                # special case: if the crud is FunctionCRUD, the resource file has to be a direct descendent.
                if parent.name == "functions" and resource_file.parent.name != "functions":
                    return None
                return parent.parent
            if parent.name == MODULES:
                return parent
        return None

    def _check_resource_folder_content(self, module_path: Path) -> tuple[None | Path, ModuleLoadingIssue | None]:
        resource_folder_names = {d.name for d in module_path.iterdir() if d.is_dir()}
        unrecognized_resource_folder_names = resource_folder_names - CRUDS_BY_FOLDER_NAME.keys()

        issue = (
            ModuleLoadingIssue(
                message=f"Module {module_path_display_name(self.organization_dir, module_path)!r} contains unrecognized resource folder(s): {', '.join(unrecognized_resource_folder_names)}"
            )
            if unrecognized_resource_folder_names
            else None
        )

        has_valid_resource_folders = bool(resource_folder_names & CRUDS_BY_FOLDER_NAME.keys())
        return (module_path if has_valid_resource_folders else None, issue)

    def _matches_selection(
        self, module_path: Path, modules_root: Path, normalized_selected: list[tuple[str, ...]] | None
    ) -> bool:
        if not normalized_selected:
            return True

        rel = module_path.relative_to(modules_root)
        rel_parts = tuple(p.lower() for p in rel.parts)
        if not rel_parts:
            # module_path is the modules_root itself
            return False
        name_lower = rel_parts[-1]

        for sel_parts in normalized_selected:
            if not sel_parts:
                return True

            if len(sel_parts) == 1 and name_lower == sel_parts[0]:
                return True

            if rel_parts[: len(sel_parts)] == sel_parts:
                return True

        return False

    def _normalize_selection(self, selected: list[str | Path]) -> list[tuple[str, ...]]:
        normalized: list[tuple[str, ...]] = []
        modules_lower = MODULES.lower()
        for sel in selected:
            if isinstance(sel, Path):
                sel_parts = sel.parts
            else:
                sel_parts = tuple(part for part in str(sel).replace("\\", "/").split("/") if part)

            sel_parts_lower = tuple(part.lower() for part in sel_parts)
            if sel_parts_lower and sel_parts_lower[0] == modules_lower:
                sel_parts_lower = sel_parts_lower[1:]

            normalized.append(sel_parts_lower)

        return normalized

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
