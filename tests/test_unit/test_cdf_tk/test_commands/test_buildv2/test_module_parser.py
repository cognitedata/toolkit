from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2._module_parser import ModuleParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import (
    AmbiguousSelection,
    MisplacedModule,
    NonExistingModuleName,
)
from cognite_toolkit._cdf_tk.constants import DEFAULT_CONFIG_FILE


class TestModuleSourceParser:
    @pytest.mark.parametrize(
        "yaml_files, expected_modules, expected_orphans",
        [
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                ],
                {"modules/moduleA": {"data_modeling": ["modules/moduleA/data_modeling/my.Space.yaml"]}},
                [],
                id="Single module with one YAML file",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/another_file.Space.yaml",
                ],
                {"modules/moduleA": {"data_modeling": ["modules/moduleA/data_modeling/my.Space.yaml"]}},
                ["modules/moduleA/another_file.Space.yaml"],
                id="Single module with one valid YAML file and one orphan",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/data_modeling/another.Space.yaml",
                    f"modules/moduleA/{DEFAULT_CONFIG_FILE}",
                ],
                {
                    "modules/moduleA": {
                        "data_modeling": [
                            "modules/moduleA/data_modeling/my.Space.yaml",
                            "modules/moduleA/data_modeling/another.Space.yaml",
                        ]
                    },
                },
                [],
                id="Single module with multiple valid YAML files and one excluded file",
            ),
        ],
    )
    def test_find_modules(
        self,
        yaml_files: list[str],
        expected_modules: dict[str, dict[str, list[str]]],
        expected_orphans: list[str],
        tmp_path: Path,
    ) -> None:
        org = tmp_path
        for yaml_file in yaml_files:
            file_path = org / yaml_file
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()

        found_modules, orphans = ModuleParser._find_modules([Path(yaml_file) for yaml_file in yaml_files], org)
        actual_modules = {
            module.as_posix(): {
                resource_folder: [file.relative_to(org).as_posix() for file in files]
                for resource_folder, files in source.resource_files_by_folder.items()
            }
            for module, source in found_modules.items()
        }
        actual_orphans = [file.as_posix() for file in orphans]
        assert actual_modules == expected_modules
        assert actual_orphans == expected_orphans

    @pytest.mark.parametrize(
        "variables, available_paths, selected_paths, expected_variables, error_messages",
        [
            pytest.param(
                {"var1": "value1", "var2": "value2", "modules": {"moduleB": {"var3": "value3"}}},
                {"", "modules", "modules/moduleA", "modules/moduleB"},
                {"", "modules", "modules/moduleA"},
                {
                    ".": {
                        None: [
                            BuildVariable(id=Path("var1"), value="value1", is_selected=True, iteration=None),
                            BuildVariable(id=Path("var2"), value="value2", is_selected=True, iteration=None),
                        ]
                    },
                    "modules/moduleB": {
                        None: [
                            BuildVariable(
                                id=Path("modules/moduleB/var3"), value="value3", is_selected=False, iteration=None
                            )
                        ]
                    },
                },
                [],
                id="Simple string variables at root level",
            ),
            pytest.param(
                {"modules": {"moduleA": {"var1": "value1"}}},
                {"", "modules", "modules/moduleA"},
                {"", "modules", "modules/moduleA"},
                {
                    "modules/moduleA": {
                        None: [
                            BuildVariable(
                                id=Path("modules/moduleA/var1"), value="value1", is_selected=True, iteration=None
                            )
                        ]
                    },
                },
                [],
                id="Nested variables in module path",
            ),
            pytest.param(
                {"modules": {"nonexistent": {"var1": "value1"}}},
                {"", "modules", "modules/moduleA"},
                {"", "modules", "modules/moduleA"},
                {},
                [
                    "Invalid variable path: modules.nonexistent. This does not correspond to the "
                    "folder structure inside the modules directory."
                ],
                id="Invalid nested path returns error",
            ),
            pytest.param(
                {"list_var": ["a", "b", "c"]},
                {""},
                {""},
                {
                    ".": {
                        None: [
                            BuildVariable(id=Path("list_var"), value=["a", "b", "c"], is_selected=True, iteration=None)
                        ]
                    },
                },
                [],
                id="List of strings as single variable",
            ),
            pytest.param(
                {"modules": {"moduleA": [{"var1": "a"}, {"var1": "b"}]}},
                {"", "modules", "modules/moduleA"},
                {"", "modules", "modules/moduleA"},
                {
                    "modules/moduleA": {
                        2: [BuildVariable(id=Path("modules/moduleA/var1"), value="b", is_selected=True, iteration=2)],
                        1: [BuildVariable(id=Path("modules/moduleA/var1"), value="a", is_selected=True, iteration=1)],
                    },
                },
                [],
                id="List of dicts creates iterations",
            ),
            pytest.param(
                {"mixed_list": ["a", {"key": "value"}]},
                {""},
                {""},
                {},
                ["Invalid variable type in list for variable mixed_list."],
                id="Mixed list types returns error",
            ),
        ],
    )
    def test_parse_variables(
        self,
        variables: dict[str, Any],
        available_paths: set[str],
        selected_paths: set[str],
        expected_variables: dict[str, dict[int | None, list[BuildVariable]]],
        error_messages: list[str],
    ) -> None:
        build_variables, errors = ModuleParser._parse_variables(
            variables, {Path(path) for path in available_paths}, {Path(path) for path in selected_paths}
        )
        actual_error_messages = [error.error.message for error in errors]
        assert actual_error_messages == error_messages
        actual_variables = {path.as_posix(): iteration_dict for path, iteration_dict in build_variables.items()}
        assert actual_variables == expected_variables


class TestGetModulePathFromResourceFilePath:
    @pytest.mark.parametrize(
        "resource_file, expected_module_path, expected_resource_folder",
        [
            pytest.param(
                Path("modules/moduleA/data_modeling/my.Space.yaml"),
                Path("modules/moduleA"),
                "data_modeling",
                id="Valid data_modeling file",
            ),
            pytest.param(
                Path("modules/nested/moduleB/transformations/my.Transformation.yaml"),
                Path("modules/nested/moduleB"),
                "transformations",
                id="Nested module with transformations",
            ),
            pytest.param(
                Path("modules/moduleA/my.yaml"),
                None,
                None,
                id="File not in resource folder",
            ),
            pytest.param(
                Path("random_file.yaml"),
                None,
                None,
                id="File at root level",
            ),
        ],
    )
    def test_get_module_path_from_resource_file_path(
        self,
        resource_file: Path,
        expected_module_path: Path | None,
        expected_resource_folder: str | None,
    ) -> None:
        module_path, resource_folder = ModuleParser._get_module_path_from_resource_file_path(resource_file)
        assert module_path == expected_module_path
        assert resource_folder == expected_resource_folder


class TestExpandParents:
    @pytest.mark.parametrize(
        "module_ids, expected_paths",
        [
            pytest.param(
                [Path("modules/moduleA")],
                {Path(""), Path("modules"), Path("modules/moduleA")},
                id="Single module",
            ),
            pytest.param(
                [Path("modules/nested/moduleA"), Path("modules/moduleB")],
                {
                    Path(""),
                    Path("modules"),
                    Path("modules/nested"),
                    Path("modules/nested/moduleA"),
                    Path("modules/moduleB"),
                },
                id="Multiple modules with different depths",
            ),
            pytest.param(
                [],
                {Path("")},
                id="Empty module list",
            ),
        ],
    )
    def test_expand_parents(self, module_ids: list[Path], expected_paths: set[Path]) -> None:
        result = ModuleParser._expand_parents(module_ids)
        assert result == expected_paths


class TestSelectModules:
    @pytest.mark.parametrize(
        "module_paths, selection, expected_selected",
        [
            pytest.param(
                [Path("modules/moduleA"), Path("modules/moduleB")],
                {Path("modules/moduleA")},
                [Path("modules/moduleA")],
                id="Select by exact path",
            ),
            pytest.param(
                [Path("modules/moduleA"), Path("modules/moduleB")],
                {"moduleA"},
                [Path("modules/moduleA")],
                id="Select by module name",
            ),
            pytest.param(
                [Path("modules/nested/moduleA"), Path("modules/moduleB")],
                {Path("modules/nested")},
                [Path("modules/nested/moduleA")],
                id="Select by parent path",
            ),
            pytest.param(
                [Path("modules/moduleA"), Path("modules/moduleB")],
                {Path("modules")},
                [Path("modules/moduleA"), Path("modules/moduleB")],
                id="Select all by common parent",
            ),
            pytest.param(
                [Path("modules/moduleA"), Path("modules/moduleB")],
                {"nonexistent"},
                [],
                id="No match returns empty",
            ),
        ],
    )
    def test_select_modules(
        self,
        module_paths: list[Path],
        selection: set[Path | str],
        expected_selected: list[Path],
    ) -> None:
        result = ModuleParser._select_modules(module_paths, selection)
        assert result == expected_selected


class TestGetNonExistingModuleNames:
    @pytest.mark.parametrize(
        "selected_module_names, available_names, expected_result",
        [
            pytest.param(
                {"moduleA", "moduleB"},
                {"moduleA", "moduleB"},
                [],
                id="All modules exist",
            ),
            pytest.param(
                {"moduleA", "moduleC"},
                {"moduleA", "moduleB"},
                [NonExistingModuleName(name="moduleC", closest_matches=["moduleB", "moduleA"])],
                id="One module does not exist with close match",
            ),
            pytest.param(
                {"xyz"},
                {"moduleA", "moduleB"},
                [NonExistingModuleName(name="xyz", closest_matches=[])],
                id="No close matches",
            ),
            pytest.param(
                set(),
                {"moduleA", "moduleB"},
                [],
                id="Empty selection",
            ),
        ],
    )
    def test_get_non_existing_module_names(
        self,
        selected_module_names: set[str],
        available_names: set[str],
        expected_result: list[NonExistingModuleName],
    ) -> None:
        result = ModuleParser._get_non_existing_module_names(selected_module_names, available_names)
        assert result == expected_result


class TestGetAmbiguousSelection:
    @pytest.mark.parametrize(
        "module_paths_by_name, selected_modules, expected_result",
        [
            pytest.param(
                {"moduleA": [Path("modules/moduleA")], "moduleB": [Path("modules/moduleB")]},
                {"moduleA"},
                [],
                id="No ambiguous modules",
            ),
            pytest.param(
                {
                    "moduleA": [Path("modules/team1/moduleA"), Path("modules/team2/moduleA")],
                    "moduleB": [Path("modules/moduleB")],
                },
                {"moduleA"},
                [
                    AmbiguousSelection(
                        name="moduleA",
                        module_paths=[Path("modules/team1/moduleA"), Path("modules/team2/moduleA")],
                        is_selected=True,
                    )
                ],
                id="Ambiguous module selected by name",
            ),
            pytest.param(
                {
                    "moduleA": [Path("modules/team1/moduleA"), Path("modules/team2/moduleA")],
                },
                {Path("modules/team1/moduleA")},
                [
                    AmbiguousSelection(
                        name="moduleA",
                        module_paths=[Path("modules/team1/moduleA"), Path("modules/team2/moduleA")],
                        is_selected=False,
                    )
                ],
                id="Ambiguous module not selected by name (selected by path)",
            ),
        ],
    )
    def test_get_ambiguous_selection(
        self,
        module_paths_by_name: dict[str, list[Path]],
        selected_modules: set[str | Path],
        expected_result: list[AmbiguousSelection],
    ) -> None:
        result = ModuleParser._get_ambiguous_selection(module_paths_by_name, selected_modules)
        assert result == expected_result


class TestGetMisplacedModules:
    @pytest.mark.parametrize(
        "module_ids, expected_result",
        [
            pytest.param(
                {Path("modules/moduleA"), Path("modules/moduleB")},
                [],
                id="No misplaced modules",
            ),
            pytest.param(
                {Path("modules/moduleA"), Path("modules/moduleA/moduleB")},
                [MisplacedModule(id=Path("modules/moduleA/moduleB"), parent_modules=[Path("modules/moduleA")])],
                id="One misplaced module that is a child of another module in the list",
            ),
        ],
    )
    def test_get_misplaced_modules(
        self,
        module_ids: set[Path],
        expected_result: list[MisplacedModule],
    ) -> None:
        result = ModuleParser._get_misplaced_modules(module_ids)
        assert result == expected_result


class TestParseVariablesEdgeCases:
    def test_numeric_variables(self) -> None:
        variables = {"int_var": 42, "float_var": 3.14, "bool_var": True}
        available_paths = {Path("")}
        selected_paths = {Path("")}

        build_variables, errors = ModuleParser._parse_variables(variables, available_paths, selected_paths)

        assert len(errors) == 0
        assert Path("") in build_variables
        vars_list = build_variables[Path("")][None]
        assert len(vars_list) == 3
        # Check values are preserved with correct types
        values = {v.id.name: v.value for v in vars_list}
        assert values == {"int_var": 42, "float_var": 3.14, "bool_var": True}

    def test_deeply_nested_variables(self) -> None:
        variables = {"modules": {"team": {"project": {"var1": "value1"}}}}
        available_paths = {Path(""), Path("modules"), Path("modules/team"), Path("modules/team/project")}
        selected_paths = available_paths.copy()

        build_variables, errors = ModuleParser._parse_variables(variables, available_paths, selected_paths)

        assert len(errors) == 0
        assert Path("modules/team/project") in build_variables
        assert build_variables[Path("modules/team/project")][None][0].value == "value1"

    def test_list_of_dicts_with_multiple_variables(self) -> None:
        variables = {"modules": {"moduleA": [{"var1": "a", "var2": "x"}, {"var1": "b", "var2": "y"}]}}
        available_paths = {Path(""), Path("modules"), Path("modules/moduleA")}
        selected_paths = available_paths.copy()

        build_variables, errors = ModuleParser._parse_variables(variables, available_paths, selected_paths)

        assert len(errors) == 0
        module_vars = build_variables.get(Path("modules/moduleA"), {})
        # Should have 2 iterations with 2 variables each
        assert len(module_vars) == 2
        assert len(module_vars[1]) == 2
        assert len(module_vars[2]) == 2
