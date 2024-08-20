from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.build import BuildCommand, _BuildState
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import (
    AmbiguousResourceFileError,
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
from cognite_toolkit._cdf_tk.prototypes import setup_robotics_loaders
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from tests import data


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )


class TestBuildCommand:
    def test_get_loader_raises_ambiguous_error(self):
        with pytest.raises(AmbiguousResourceFileError) as e:
            BuildCommand()._get_loader(
                "transformations",
                destination=Path("transformation") / "notification.yaml",
                source_path=Path("my_module") / "transformations" / "notification.yaml",
            )
        assert "Ambiguous resource file" in str(e.value)

    def test_module_not_found_error(self, tmp_path: Path) -> None:
        with pytest.raises(ToolkitMissingModuleError):
            BuildCommand(print_warning=False).execute(
                verbose=False,
                build_dir=tmp_path,
                source_path=data.PROJECT_WITH_BAD_MODULES,
                build_env_name="no_module",
                no_clean=False,
            )

    def test_module_with_non_resource_directories(self, tmp_path: Path) -> None:
        cmd = BuildCommand(print_warning=False)
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            source_path=data.PROJECT_WITH_BAD_MODULES,
            build_env_name="ill_module",
            no_clean=False,
        )

        assert len(cmd.warning_list) >= 1
        assert (
            LowSeverityWarning(
                f"Module 'ill_made_module' has non-resource directories: ['spaces']. {ModuleDefinition.short()}"
            )
            in cmd.warning_list
        )

    def test_custom_project_no_warnings(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        setup_robotics_loaders.setup_robotics_loaders()

        cmd = BuildCommand(print_warning=False)
        monkeypatch.setenv("CDF_PROJECT", "some-project")
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            source_path=data.PROJECT_NO_COGNITE_MODULES,
            build_env_name="dev",
            no_clean=False,
        )

        assert not cmd.warning_list, f"No warnings should be raised. Got warnings: {cmd.warning_list}"
        # There are two transformations in the project, expect two transformation files
        transformation_files = [
            f
            for f in (tmp_path / "transformations").iterdir()
            if f.is_file() and TransformationLoader.is_supported_file(f)
        ]
        assert len(transformation_files) == 2
        sql_files = [f for f in (tmp_path / "transformations").iterdir() if f.is_file() and f.suffix == ".sql"]
        assert len(sql_files) == 2


def valid_yaml_semantics_test_cases() -> Iterable[pytest.ParameterSet]:
    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
- dbName: src:002:weather:rawdb:state
- dbName: uc:001:demand:rawdb:state
- dbName: in:all:rawdb:state
- dbName: src:001:sap:rawdb
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
tableName: myTable
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database with table",
    )

    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
  tableName: myTable
- dbName: src:002:weather:rawdb:state
  tableName: myOtherTable
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases with table",
    )


class TestCheckYamlSemantics:
    @pytest.mark.parametrize("raw_yaml, source_path", list(valid_yaml_semantics_test_cases()))
    def test_valid_yaml(self, raw_yaml: str, source_path: Path, dummy_environment: Environment):
        state = _BuildState.create(BuildConfigYAML(dummy_environment, filepath=Path("dummy"), variables={}))
        cmd = BuildCommand(print_warning=False)
        # Only used in error messages
        destination = Path("build/raw/raw.yaml")
        yaml_warnings = cmd.validate(raw_yaml, source_path, destination, state, False)
        assert not yaml_warnings


@pytest.fixture()
def my_config():
    return {
        "top_variable": "my_top_variable",
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent": {"child": {"child_variable": "my_child_variable"}},
    }


def test_split_config(my_config: dict[str, Any]) -> None:
    expected = {
        "": {"top_variable": "my_top_variable"},
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent.child": {"child_variable": "my_child_variable"},
    }
    actual = _Helpers.to_variables_by_module_path(my_config)

    assert actual == expected


def test_create_local_config(my_config: dict[str, Any]):
    configs = _Helpers.to_variables_by_module_path(my_config)

    local_config = _Helpers.create_local_config(configs, Path("parent/child/auth/"))

    assert dict(local_config.items()) == {"top_variable": "my_top_variable", "child_variable": "my_child_variable"}


class TestBuildState:
    def test_replace_preserve_data_type(self):
        source_yaml = """text: {{ my_text }}
bool: {{ my_bool }}
integer: {{ my_integer }}
float: {{ my_float }}
digit_string: {{ my_digit_string }}
quoted_string: "{{ my_quoted_string }}"
list: {{ my_list }}
null_value: {{ my_null_value }}
single_quoted_string: '{{ my_single_quoted_string }}'
composite: 'some_prefix_{{ my_composite }}'
prefix_text: {{ my_prefix_text }}
suffix_text: {{ my_suffix_text }}
"""
        variables = {
            "my_text": "some text",
            "my_bool": True,
            "my_integer": 123,
            "my_float": 123.456,
            "my_digit_string": "123",
            "my_quoted_string": "456",
            "my_list": ["one", "two", "three"],
            "my_null_value": None,
            "my_single_quoted_string": "789",
            "my_composite": "the suffix",
            "my_prefix_text": "prefix:",
            "my_suffix_text": ":suffix",
        }
        state = _BuildState.create(
            BuildConfigYAML(
                Environment("dev", "my_project", "dev", ["none"]),
                Path("dummy"),
                {"modules": {"my_module": variables}},
            )
        )
        state.update_local_variables(Path("modules") / "my_module")

        result = state.replace_variables(source_yaml)

        loaded = yaml.safe_load(result)
        assert loaded == {
            "text": "some text",
            "bool": True,
            "integer": 123,
            "float": 123.456,
            "digit_string": "123",
            "quoted_string": "456",
            "list": ["one", "two", "three"],
            "null_value": None,
            "single_quoted_string": "789",
            "composite": "some_prefix_the suffix",
            "prefix_text": "prefix:",
            "suffix_text": ":suffix",
        }

    def test_replace_not_preserve_type(self):
        source_yaml = """dataset_id('{{dataset_external_id}}')"""
        variables = {
            "dataset_external_id": "ds_external_id",
        }
        state = _BuildState.create(
            BuildConfigYAML(
                Environment("dev", "my_project", "dev", ["none"]),
                Path("dummy"),
                {"modules": {"my_module": variables}},
            )
        )
        state.update_local_variables(Path("modules") / "my_module")

        result = state.replace_variables(source_yaml, file_suffix=".sql")

        assert result == "dataset_id('ds_external_id')"
