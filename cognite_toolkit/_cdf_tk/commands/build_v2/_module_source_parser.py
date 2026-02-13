from typing import Any

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    AbsoluteDirPath,
    InsightList,
    ModelSyntaxError,
    ModuleSource,
    RelativeDirPath,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    def __init__(self, selected_modules: set[RelativeDirPath | str], organization_dir: AbsoluteDirPath) -> None:
        self.selected_modules = selected_modules
        self.organization_dir = organization_dir
        self.errors = InsightList()

    def parse(self, yaml_files: list[RelativeDirPath], variables: dict[str, Any]) -> list[ModuleSource]:
        files_by_module = self._find_modules(yaml_files)
        errors = self._validate_modules(list(files_by_module.keys()), self.selected_modules)
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
    def _find_modules(cls, yaml_files: list[RelativeDirPath]) -> dict[RelativeDirPath, list[RelativeDirPath]]:
        """Organizes YAML files by their module (top-level folder in the modules directory)."""
        raise NotImplementedError()

    @classmethod
    def _validate_modules(
        cls, module_paths: list[RelativeDirPath], selection: set[RelativeDirPath | str]
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
