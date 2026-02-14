from pathlib import Path
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2._module_source_parser import ModuleSourceParser
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable
from cognite_toolkit._cdf_tk.constants import DEFAULT_CONFIG_FILE


class TestModuleSourceParser:
    @pytest.mark.parametrize(
        "yaml_files, expected_modules, expected_orphans",
        [
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                ],
                {"modules/moduleA": ["modules/moduleA/data_modeling/my.Space.yaml"]},
                [],
                id="Single module with one YAML file",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/another_file.Space.yaml",
                ],
                {"modules/moduleA": ["modules/moduleA/data_modeling/my.Space.yaml"]},
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
                    "modules/moduleA": [
                        "modules/moduleA/data_modeling/my.Space.yaml",
                        "modules/moduleA/data_modeling/another.Space.yaml",
                    ],
                },
                [],
                id="Single module with multiple valid YAML files and one excluded file",
            ),
        ],
    )
    def test_find_modules(
        self, yaml_files: list[str], expected_modules: dict[str, list[str]], expected_orphans: list[str]
    ) -> None:
        found_modules, orphans = ModuleSourceParser._find_modules([Path(yaml_file) for yaml_file in yaml_files])
        actual_modules = {
            module.as_posix(): [file.as_posix() for file in files] for module, files in found_modules.items()
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
                    ".": [
                        BuildVariable(id=Path("var1"), value="value1", is_selected=True, iteration=None),
                        BuildVariable(id=Path("var2"), value="value2", is_selected=True, iteration=None),
                    ],
                    "modules/moduleB": [
                        BuildVariable(
                            id=Path("modules/moduleB/var3"), value="value3", is_selected=False, iteration=None
                        )
                    ],
                },
                [],
                id="Simple string variables at root level",
            ),
            pytest.param(
                {"modules": {"moduleA": {"var1": "value1"}}},
                {"", "modules", "modules/moduleA"},
                {"", "modules", "modules/moduleA"},
                {
                    "modules/moduleA": [
                        BuildVariable(id=Path("modules/moduleA/var1"), value="value1", is_selected=True, iteration=None)
                    ],
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
                    ".": [BuildVariable(id=Path("list_var"), value=["a", "b", "c"], is_selected=True, iteration=None)],
                },
                [],
                id="List of strings as single variable",
            ),
            pytest.param(
                {"modules": {"moduleA": [{"var1": "a"}, {"var1": "b"}]}},
                {"", "modules", "modules/moduleA"},
                {"", "modules", "modules/moduleA"},
                {
                    "modules/moduleA": [
                        BuildVariable(id=Path("modules/moduleA/var1"), value="b", is_selected=True, iteration=2),
                        BuildVariable(id=Path("modules/moduleA/var1"), value="a", is_selected=True, iteration=1),
                    ],
                },
                [],
                id="List of dicts creates iterations",
            ),
            pytest.param(
                {"mixed_list": ["a", {"key": "value"}]},
                {"", "mixed_list"},
                {"", "mixed_list"},
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
        expected_variables: dict[str, list[BuildVariable]],
        error_messages: list[str],
    ) -> None:
        build_variables, errors = ModuleSourceParser._parse_variables(
            variables, {Path(path) for path in available_paths}, {Path(path) for path in selected_paths}
        )
        actual_error_messages = [error.message for error in errors]
        assert actual_error_messages == error_messages
        actual_variables = {path.as_posix(): var_list for path, var_list in build_variables.items()}
        assert actual_variables == expected_variables

    @pytest.mark.parametrize(
        "variables, available_modules, selected_modules, expected_variables, error_messages",
        [
            pytest.param(
                {"var1": "value1"},
                {"modules/moduleA"},
                {"modules/moduleA"},
                {
                    "modules/moduleA": {
                        0: [BuildVariable(id=Path("var1"), value="value1", is_selected=True, iteration=None)],
                    },
                },
                [],
                id="Root variable applied to single module",
            ),
            pytest.param(
                {"modules": {"moduleA": {"var1": "value1"}}},
                {"modules/moduleA"},
                {"modules/moduleA"},
                {
                    "modules/moduleA": {
                        0: [
                            BuildVariable(
                                id=Path("modules/moduleA/var1"), value="value1", is_selected=True, iteration=None
                            )
                        ],
                    },
                },
                [],
                id="Module-specific variable",
            ),
            pytest.param(
                {"var1": "root", "modules": {"moduleA": {"var2": "moduleA_value"}}},
                {"modules/moduleA", "modules/moduleB"},
                {"modules/moduleA"},
                {
                    "modules/moduleA": {
                        0: [
                            BuildVariable(id=Path("var1"), value="root", is_selected=True, iteration=None),
                            BuildVariable(
                                id=Path("modules/moduleA/var2"), value="moduleA_value", is_selected=True, iteration=None
                            ),
                        ],
                    },
                },
                [],
                id="Root and module-specific variables combined",
            ),
            pytest.param(
                {"modules": {"moduleA": [{"var1": "a"}, {"var1": "b"}]}},
                {"modules/moduleA"},
                {"modules/moduleA"},
                {
                    "modules/moduleA": {
                        1: [BuildVariable(id=Path("modules/moduleA/var1"), value="a", is_selected=True, iteration=1)],
                        2: [BuildVariable(id=Path("modules/moduleA/var1"), value="b", is_selected=True, iteration=2)],
                    },
                },
                [],
                id="Module with iterations",
            ),
        ],
    )
    def test_parse_module_variables(
        self,
        variables: dict[str, Any],
        available_modules: set[str],
        selected_modules: set[str],
        expected_variables: dict[str, list[list[BuildVariable]]],
        error_messages: list[str],
    ) -> None:
        module_variables, errors = ModuleSourceParser._parse_module_variables(
            variables, {Path(path) for path in available_modules}, {Path(path) for path in selected_modules}
        )
        actual_error_messages = [error.message for error in errors]
        assert actual_error_messages == error_messages
        actual_variables = {path.as_posix(): var_list for path, var_list in module_variables.items()}
        assert actual_variables == expected_variables
