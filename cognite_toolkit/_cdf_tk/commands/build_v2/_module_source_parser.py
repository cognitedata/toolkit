import difflib
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, cast

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildSourceFiles,
    ModelSyntaxWarning,
    ModuleSource,
    RelativeDirPath,
    RelativeFilePath,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    AmbiguousSelection,
    BuildSource,
    BuildVariable,
    InvalidBuildVariable,
    MisplacedModule,
    NonExistingModuleName,
)
from cognite_toolkit._cdf_tk.constants import EXCL_FILES, MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA, ResourceTypes


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    @classmethod
    def parse(cls, build: BuildSourceFiles) -> BuildSource:
        source_by_module_id, orphan_yaml_files = cls._find_modules(build.yaml_files, build.organization_dir)

        module_ids = list(source_by_module_id.keys())
        available_paths = cls._expand_parents(module_ids)
        selected_modules = cls._select_modules(module_ids, build.selected_modules)
        selected_paths = cls._expand_parents(selected_modules)

        module_paths_by_name: dict[str, list[RelativeDirPath]] = defaultdict(list)
        for module_path in module_ids:
            module_paths_by_name[module_path.name].append(module_path)

        build_variables, invalid_variables = cls._parse_variables(build.variables, available_paths, selected_paths)

        module_sources: list[ModuleSource] = []
        for module in selected_modules:
            source = source_by_module_id[module]
            module_specific_variables: dict[int | None, list[BuildVariable]] = defaultdict(list)
            for path in [module, *module.parents]:
                if path_variables := build_variables.get(path):
                    for iteration, variables in path_variables.items():
                        module_specific_variables[iteration].extend(variables)

            if module_specific_variables:
                for iteration, variables in module_specific_variables.items():
                    module_sources.append(
                        source.model_copy(update={"variables": variables, "iteration": iteration or 0})
                    )
            else:
                module_sources.append(source)

        return BuildSource(
            module_dir=build.module_dir,
            modules=module_sources,
            invalid_variables=invalid_variables,
            non_existing_module_names=cls._get_non_existing_module_names(
                {name for name in build.selected_modules if isinstance(name, str)}, set(module_paths_by_name.keys())
            ),
            misplaced_modules=cls._get_misplaced_modules(set(module_ids)),
            ambiguous_selection=cls._get_ambiguous_selection(module_paths_by_name, build.selected_modules),
            orphan_yaml_files=orphan_yaml_files,
        )

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
    def _expand_parents(cls, module_ids: Sequence[Path]) -> set[Path]:
        return {Path("")} | set(module_ids) | {parent for module in module_ids for parent in module.parents}

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
    def _get_non_existing_module_names(
        cls, selected_module_names: set[str], available_names: set[str]
    ) -> list[NonExistingModuleName]:
        non_existing: list[NonExistingModuleName] = []
        for name in sorted(selected_module_names - available_names):
            closest_matches = list(difflib.get_close_matches(name, available_names))
            non_existing.append(NonExistingModuleName(name=name, closest_matches=closest_matches))
        return non_existing

    @classmethod
    def _get_misplaced_modules(cls, module_ids: set[RelativeDirPath]) -> list[MisplacedModule]:
        misplaced_modules: list[MisplacedModule] = []
        for module_path in sorted(module_ids):
            module_parents = set(module_path.parents)
            if parent_modules := (module_ids & module_parents):
                misplaced_modules.append(MisplacedModule(id=module_path, parent_modules=sorted(parent_modules)))
        return misplaced_modules

    @classmethod
    def _get_ambiguous_selection(
        cls, module_paths_by_name: dict[str, list[RelativeDirPath]], selected_modules: set[str | RelativeDirPath]
    ) -> list[AmbiguousSelection]:
        return [
            AmbiguousSelection(
                name=name,
                module_paths=module_paths,
                is_selected=name in selected_modules,
            )
            for name, module_paths in module_paths_by_name.items()
            if len(module_paths) > 1
        ]

    @classmethod
    def _parse_variables(
        cls, variables: dict[str, Any], available_paths: set[RelativeDirPath], selected_paths: set[RelativeDirPath]
    ) -> tuple[dict[RelativeDirPath, dict[int | None, list[BuildVariable]]], list[InvalidBuildVariable]]:
        variables_by_path_and_iteration: dict[RelativeDirPath, dict[int | None, list[BuildVariable]]] = defaultdict(
            lambda: defaultdict(list)
        )
        invalid_variables: list[InvalidBuildVariable] = []
        to_check: list[tuple[RelativeDirPath, int | None, dict[str, Any]]] = [(Path(""), None, variables)]
        while to_check:
            path, iteration, subdict = to_check.pop()
            for key, value in subdict.items():
                subpath = path / key
                if isinstance(value, str | float | int | bool):
                    variables_by_path_and_iteration[path][iteration].append(
                        BuildVariable(id=subpath, value=value, is_selected=path in selected_paths, iteration=iteration)
                    )
                elif isinstance(value, dict):
                    if subpath in available_paths:
                        to_check.append((subpath, iteration, value))
                    else:
                        invalid_variables.append(
                            InvalidBuildVariable(
                                id=subpath,
                                value=str(value),
                                is_selected=path in selected_paths,
                                iteration=iteration,
                                error=ModelSyntaxWarning(
                                    code=cls.VARIABLE_ERROR_CODE,
                                    message=f"Invalid variable path: {'.'.join(subpath.parts)}. This does not correspond to the "
                                    f"folder structure inside the {MODULES} directory.",
                                    fix="Ensure that the variable paths correspond to the folder structure inside the modules directory.",
                                ),
                            )
                        )
                elif isinstance(value, list):
                    if all(isinstance(item, str | float | int | bool) for item in value):
                        variables_by_path_and_iteration[path][iteration].append(
                            BuildVariable(
                                id=subpath, value=value, is_selected=path in selected_paths, iteration=iteration
                            )
                        )
                    elif all(isinstance(item, dict) for item in value):
                        for idx, item in enumerate(value, start=1):
                            to_check.append((subpath, idx, item))
                    else:
                        invalid_variables.append(
                            InvalidBuildVariable(
                                id=subpath,
                                value=str(value),
                                is_selected=path in selected_paths,
                                iteration=iteration,
                                error=ModelSyntaxWarning(
                                    code=cls.VARIABLE_ERROR_CODE,
                                    message=f"Invalid variable type in list for variable {'.'.join(subpath.parts)}.",
                                    fix="Ensure that all items in the list are of the same supported type either (str, int, float, bool) or dict.",
                                ),
                            )
                        )
                else:
                    raise NotImplementedError(f"Unsupported variable type: {type(value)} for variable {subpath}")
        return variables_by_path_and_iteration, invalid_variables
