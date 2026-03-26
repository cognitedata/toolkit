from collections import defaultdict
from collections.abc import Iterable
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    AbsoluteDirPath,
    InsightList,
    ModelSyntaxWarning,
    ModuleSource,
    RelativeDirPath,
    RelativeFilePath,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    AmbiguousSelection,
    BuildVariable,
    InvalidBuildVariable,
    MisplacedModule,
)
from cognite_toolkit._cdf_tk.constants import EXCL_FILES, MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA, ResourceTypes


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    def __init__(self, selected_modules: set[RelativeDirPath | str], organization_dir: AbsoluteDirPath) -> None:
        self.selected_modules = selected_modules
        self.organization_dir = organization_dir
        self.errors = InsightList()
        self.ambiguous_selection: list[AmbiguousSelection] = []
        self.misplaced_modules: list[MisplacedModule] = []
        self.non_existing_module_names: list[str] = []
        self.orphan_yaml_files: list[AbsoluteDirPath] = []
        self.invalid_variables: list[InvalidBuildVariable] = []

    def parse(self, yaml_files: list[RelativeFilePath], variables: dict[str, Any]) -> list[ModuleSource]:
        source_by_module_id = self._find_modules(yaml_files, self.organization_dir)
        module_ids = list(source_by_module_id.keys())
        selected_modules = self._select_modules(module_ids, self.selected_modules)
        available_paths = {Path("")} | set(module_ids) | {parent for module in module_ids for parent in module.parents}
        selected_paths = (
            {Path("")} | set(selected_modules) | {parent for module in selected_modules for parent in module.parents}
        )
        module_paths_by_name: dict[str, list[RelativeDirPath]] = defaultdict(list)
        for module_path in module_ids:
            module_paths_by_name[module_path.name].append(module_path)

        self.ambiguous_selection = [
            AmbiguousSelection(
                name=name,
                module_paths=module_paths,
                is_selected=name in self.selected_modules,
            )
            for name, module_paths in module_paths_by_name.items()
            if len(module_paths) > 1
        ]

        for module_path in module_ids:
            if parent_modules := (available_paths.intersection(module_paths_by_name[module_path.name]) - {module_path}):
                self.misplaced_modules.append(MisplacedModule(id=module_path, parent_modules=sorted(parent_modules)))

        self.non_existing_modules = sorted(
            {name for name in self.selected_modules if isinstance(name, str)} - set(module_paths_by_name.keys())
        )

        build_variables = self._parse_variables(variables, available_paths, selected_paths)
        return self._create_module_soruces(selected_modules, source_by_module_id, build_variables)

    def _create_module_soruces(
        self,
        selected_modules: list[Path],
        source_by_module_id: dict[Path, ModuleSource],
        build_variables: dict[Path, list[BuildVariable]],
    ) -> list[ModuleSource]:
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

    def _find_modules(
        self, yaml_files: list[RelativeFilePath], organization_dir: Path
    ) -> dict[RelativeDirPath, ModuleSource]:
        """Organizes YAML files by their module (top-level folder in the modules directory)."""
        source_by_module_id: dict[RelativeDirPath, ModuleSource] = {}
        for yaml_file in yaml_files:
            if yaml_file.name in EXCL_FILES:
                continue
            relative_module_path, resource_folder = self._get_module_path_from_resource_file_path(yaml_file)
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
                self.orphan_yaml_files.append(yaml_file)
        return source_by_module_id

    @staticmethod
    def _get_module_path_from_resource_file_path(resource_file: Path) -> tuple[Path | None, ResourceTypes | None]:
        for parent in resource_file.parents:
            if parent.name in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA:
                # We know that all keys in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA are valid ResourceTypes,
                # so this cast is safe.
                return parent.parent, cast(ResourceTypes, parent.name)
        return None, None

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

    def _parse_variables(
        self, variables: dict[str, Any], available_paths: set[RelativeDirPath], selected_paths: set[RelativeDirPath]
    ) -> dict[RelativeDirPath, list[BuildVariable]]:
        variables_by_path: dict[RelativeDirPath, list[BuildVariable]] = defaultdict(list)
        to_check: list[tuple[RelativeDirPath, int | None, dict[str, Any]]] = [(Path(""), None, variables)]
        while to_check:
            path, iteration, subdict = to_check.pop()
            for key, value in subdict.items():
                subpath = path / key
                if isinstance(value, str | float | int | bool):
                    variables_by_path[path].append(
                        BuildVariable(id=subpath, value=value, is_selected=path in selected_paths, iteration=iteration)
                    )
                elif isinstance(value, dict):
                    if subpath in available_paths:
                        to_check.append((subpath, iteration, value))
                    else:
                        self.invalid_variables.append(
                            InvalidBuildVariable(
                                id=subpath,
                                value=str(value),
                                is_selected=path in selected_paths,
                                iteration=iteration,
                                error=ModelSyntaxWarning(
                                    code=self.VARIABLE_ERROR_CODE,
                                    message=f"Invalid variable path: {'.'.join(subpath.parts)}. This does not correspond to the "
                                    f"folder structure inside the {MODULES} directory.",
                                    fix="Ensure that the variable paths correspond to the folder structure inside the modules directory.",
                                ),
                            )
                        )
                elif isinstance(value, list):
                    if all(isinstance(item, str | float | int | bool) for item in value):
                        variables_by_path[path].append(
                            BuildVariable(
                                id=subpath, value=value, is_selected=path in selected_paths, iteration=iteration
                            )
                        )
                    elif all(isinstance(item, dict) for item in value):
                        for idx, item in enumerate(value, start=1):
                            to_check.append((subpath, idx, item))
                    else:
                        self.invalid_variables.append(
                            InvalidBuildVariable(
                                id=subpath,
                                value=value,
                                is_selected=path in selected_paths,
                                iteration=iteration,
                                error=ModelSyntaxWarning(
                                    code=self.VARIABLE_ERROR_CODE,
                                    message=f"Invalid variable type in list for variable {'.'.join(subpath.parts)}.",
                                    fix="Ensure that all items in the list are of the same supported type either (str, int, float, bool) or dict.",
                                ),
                            )
                        )
                else:
                    raise NotImplementedError(f"Unsupported variable type: {type(value)} for variable {subpath}")
        return variables_by_path

    @classmethod
    def _organize_variables_by_module(
        cls, variables_by_path: dict[RelativeDirPath, list[BuildVariable]], selected_modules: set[RelativeDirPath]
    ) -> dict[RelativeDirPath, dict[int, list[BuildVariable]]]:
        module_path_by_relative_paths: dict[frozenset[RelativeDirPath], RelativeDirPath] = {
            frozenset([module, *list(module.parents)]): module for module in selected_modules
        }
        variables_by_module: dict[RelativeDirPath, dict[int, list[BuildVariable]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for variable_path, variables in variables_by_path.items():
            for module_paths, module in module_path_by_relative_paths.items():
                if variable_path in module_paths:
                    for iteration, variable in groupby(
                        sorted(variables, key=lambda v: v.iteration or 0), key=lambda v: v.iteration or 0
                    ):
                        variables_by_module[module][iteration or 0].extend(variable)
        return dict(variables_by_module)
