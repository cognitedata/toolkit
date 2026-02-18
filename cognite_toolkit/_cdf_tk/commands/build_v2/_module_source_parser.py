from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    AbsoluteDirPath,
    InsightList,
    ModelSyntaxError,
    ModuleSource,
    RelativeDirPath,
    RelativeFilePath,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable
from cognite_toolkit._cdf_tk.constants import EXCL_FILES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA, ResourceTypes


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    def __init__(self, selected_modules: set[RelativeDirPath | str], organization_dir: AbsoluteDirPath) -> None:
        self.selected_modules = selected_modules
        self.organization_dir = organization_dir
        self.errors = InsightList()

    def parse(self, yaml_files: list[RelativeFilePath], variables: dict[str, Any]) -> list[ModuleSource]:
        source_by_module_id, orphans = self._find_modules(yaml_files, self.organization_dir)
        module_ids = list(source_by_module_id.keys())
        errors = self._validate_modules(module_ids, self.selected_modules, orphans)
        if errors:
            self.errors.extend(errors)
            return []
        selected_modules = self._select_modules(module_ids, self.selected_modules)
        build_variables, errors = self._parse_variables(variables, set(module_ids), set(selected_modules))
        if errors:
            self.errors.extend(errors)
            return []
        module_sources: list[ModuleSource] = []
        for module in selected_modules:
            source = source_by_module_id[module]
            module_build_variables = build_variables.get(module, [])
            if module_build_variables:
                for iteration, module_variable in enumerate(module_build_variables, start=1):
                    module_sources.append(
                        source.model_copy(update={"variables": module_variable, "iteration": iteration})
                    )
            else:
                module_sources.append(source)
        return module_sources

    @classmethod
    def _find_modules(
        cls, yaml_files: list[RelativeFilePath], organization_dir: Path
    ) -> tuple[dict[RelativeDirPath, ModuleSource], list[RelativeDirPath]]:
        """Organizes YAML files by their module (top-level folder in the modules directory)."""
        source_by_module_id: dict[RelativeDirPath, ModuleSource] = {}
        orphan_files: list[RelativeDirPath] = []
        for yaml_file in yaml_files:
            if yaml_file.name in EXCL_FILES:
                continue
            relative_module_path, resource_folder = cls._get_module_path_from_resource_file_path(yaml_file)
            if relative_module_path and resource_folder:
                if relative_module_path not in source_by_module_id:
                    source_by_module_id[relative_module_path] = ModuleSource(
                        path=organization_dir / relative_module_path,
                        id=relative_module_path,
                    )
                source = source_by_module_id[relative_module_path]
                if resource_folder not in source.resource_files_by_folder:
                    source.resource_files_by_folder[resource_folder] = []
                source.resource_files_by_folder[resource_folder].append(organization_dir / yaml_file)
            else:
                orphan_files.append(yaml_file)
        return source_by_module_id, orphan_files

    @staticmethod
    def _get_module_path_from_resource_file_path(resource_file: Path) -> tuple[Path | None, ResourceTypes | None]:
        for parent in resource_file.parents:
            if parent.name in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA:
                # We know that all keys in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA are valid ResourceTypes,
                # so this cast is safe.
                return parent.parent, cast(ResourceTypes, parent.name)
        return None, None

    @classmethod
    def _validate_modules(
        cls,
        module_paths: list[RelativeDirPath],
        selection: set[RelativeDirPath | str],
        orphan_files: list[RelativeDirPath],
    ) -> list[ModelSyntaxError]:
        # Todo: Check the following
        # 1) If any module is inside another module. -> Error.
        # 2) If any selection by name is ambiguous (matches multiple found modules) -> Error.
        # 3) Any selected module by name that does not match any found module -> Error.
        # 4) If any orphan files that have a resource kind set are found -> Error.
        return []

    @classmethod
    def _select_modules(
        cls, module_paths: Iterable[RelativeDirPath], selection: set[RelativeDirPath | str]
    ) -> list[RelativeDirPath]:
        return [
            module_path
            for module_path in module_paths
            if module_path in selection
            or module_path.name in selection
            or any(parent in selection for parent in module_path.parents)
        ]

    @classmethod
    def _parse_variables(
        cls,
        variables: dict[str, Any],
        available_modules: set[RelativeDirPath],
        selected_modules: set[RelativeDirPath],
    ) -> tuple[dict[RelativeDirPath, list[list[BuildVariable]]], list[ModelSyntaxError]]:
        return {}, []
