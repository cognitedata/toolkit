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
        build_variables, errors = self._parse_module_variables(variables, set(files_by_module.keys()), set(selected_modules))
        if errors:
            self.errors.extend(errors)
            return []
        return self._create_module_sources(build_variables, files_by_module, selected_modules)

    def _create_module_sources(
        self,
        build_variables: dict[Path, list[list[BuildVariable]]],
        files_by_module: dict[Path, list[Path]],
        selected_modules: list[Path],
    ) -> list[ModuleSource]:
        module_sources: list[ModuleSource] = []
        for module in selected_modules:
            source = ModuleSource(
                path=self.organization_dir / module,
                id=module,
                resource_files=[self.organization_dir / resource_file for resource_file in files_by_module[module]],
            )
            module_build_variables = build_variables.get(module, [])
            if module_build_variables:
                for iteration, module_variable in enumerate(module_build_variables, start=1):
                    module_sources.append(
                        source.model_copy(
                            update={
                                "variables": module_variable,
                                "iteration": iteration,
                            }
                        )
                    )
            else:
                module_sources.append(source)
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
        # Todo: Check the following
        # 1) If any module is inside another module. -> Error.
        # 2) If any selection by name is ambiguous (matches multiple found modules) -> Error.
        # 3) Any selected module by name that does not match any found module -> Error.
        # 4) If any orphan files that have a resource kind set are found -> Error.
        return []

    @classmethod
    def _select_modules(
        cls, files_by_module: dict[RelativeDirPath, list[RelativeDirPath]], selection: set[RelativeDirPath | str]
    ) -> list[RelativeDirPath]:
        return [
            module_path
            for module_path in files_by_module.keys()
            if module_path in selection
            or module_path.name in selection
            or any(parent in selection for parent in module_path.parents)
        ]

    @classmethod
    def _parse_module_variables(
        cls,
        variables: dict[str, Any],
        available_modules: set[RelativeDirPath],
        selected_modules: set[RelativeDirPath],
    ) -> tuple[dict[RelativeDirPath, list[list[BuildVariable]]], list[ModelSyntaxError]]:
        all_available_paths = {Path("")} | available_modules | {parent for module in available_modules for parent in
                                                                module.parents}
        selected_paths = {Path("")} | selected_modules | {parent for module in selected_modules for parent in module.parents}
        parsed_variable, errors = cls._parse_ariables(variables, all_available_paths, selected_paths)
        variable_by_module = cls._organize_variables_by_module(parsed_variable, selected_modules)

        return {}, errors

    @classmethod
    def _parse_variables(cls, variables: dict[str, Any], available_paths: set[RelativeDirPath], selected_paths: set[RelativeDirPath]) -> tuple[dict[RelativeDirPath, list[BuildVariable]], list[ModelSyntaxError]]:
        variables_by_path: dict[RelativeDirPath, list[BuildVariable]] = defaultdict(list)
        errors: list[ModelSyntaxError] = []
        to_check: list[tuple[RelativeDirPath, dict[str, Any]]] = [(Path(""), variables)]
        while to_check:
            raise NotImplementedError()

    @classmethod
    def _organize_variables_by_module(cls, variables_by_path: dict[RelativeDirPath, list[BuildVariable]], selected_modules: set[RelativeDirPath]) -> dict[RelativeDirPath, list[list[BuildVariable]]]:
        variable_by_module: dict[RelativeDirPath, list[list[BuildVariable]]] = defaultdict(list)
        for module in selected_modules:
            module_variables = []
            for path, variables in variables_by_path.items():
                if path == Path("") or path in module.parents or path == module:
                    module_variables.extend(variables)
            variable_by_module[module].append(module_variables)
        return variable_by_module
