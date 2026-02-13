from collections import defaultdict
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    AbsoluteDirPath,
    InsightList,
    ModelSyntaxError,
    ModuleSource,
    RelativeDirPath,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable
from cognite_toolkit._cdf_tk.constants import EXCL_FILES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    def __init__(self, selected_modules: set[RelativeDirPath | str], organization_dir: AbsoluteDirPath) -> None:
        self.selected_modules = selected_modules
        self.organization_dir = organization_dir
        self.errors = InsightList()

    def parse(self, yaml_files: list[RelativeDirPath], variables: dict[str, Any]) -> list[ModuleSource]:
        files_by_module, orphans = self._find_modules(yaml_files)
        errors = self._validate_modules(list(files_by_module.keys()), self.selected_modules, orphans)
        if errors:
            self.errors.extend(errors)
            return []
        selected_modules = self._select_modules(files_by_module, self.selected_modules)
        build_variables, errors = self._parse_variables(variables, set(files_by_module.keys()), set(selected_modules))
        if errors:
            self.errors.extend(errors)
            return []
        module_sources: list[ModuleSource] = []
        for module in selected_modules:
            module_build_variables = build_variables.get(module, [])
            for iteration, module_variable in enumerate(module_build_variables, start=1):
                module_sources.append(
                    ModuleSource(
                        path=self.organization_dir / module,
                        id=module,
                        resource_files=files_by_module[module],
                        variables=module_variable,
                        iteration=iteration,
                    )
                )
        return module_sources

    @classmethod
    def _find_modules(
        cls, yaml_files: list[RelativeDirPath]
    ) -> tuple[dict[RelativeDirPath, list[RelativeDirPath]], list[RelativeDirPath]]:
        """Organizes YAML files by their module (top-level folder in the modules directory)."""
        files_by_module: dict[RelativeDirPath, list[RelativeDirPath]] = defaultdict(list)
        orphan_files: list[RelativeDirPath] = []
        for yaml_file in yaml_files:
            if yaml_file.name in EXCL_FILES:
                continue
            module_path = cls._get_module_path_from_resource_file_path(yaml_file)
            if module_path:
                files_by_module[module_path].append(yaml_file)
            else:
                orphan_files.append(yaml_file)
        return dict(files_by_module), orphan_files

    @staticmethod
    def _get_module_path_from_resource_file_path(resource_file: Path) -> Path | None:
        for parent in resource_file.parents:
            if parent.name in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA:
                return parent.parent
        return None

    @classmethod
    def _validate_modules(
        cls,
        module_paths: list[RelativeDirPath],
        selection: set[RelativeDirPath | str],
        orphan_files: list[RelativeDirPath],
    ) -> list[ModelSyntaxError]:
        raise NotImplementedError()

    @classmethod
    def _select_modules(
        cls, files_by_module: dict[RelativeDirPath, list[RelativeDirPath]], selection: set[RelativeDirPath | str]
    ) -> list[RelativeDirPath]:
        raise NotImplementedError()

    @classmethod
    def _parse_variables(
        cls,
        variables: dict[str, Any],
        available_modules: set[RelativeDirPath],
        selected_modules: set[RelativeDirPath],
    ) -> tuple[dict[RelativeDirPath, list[list[BuildVariable]]], list[ModelSyntaxError]]:
        raise NotImplementedError()
