from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
import yaml

from cognite_toolkit._cdf_tk.commands.build import BuildCommand, _BuildState, _Helpers
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment
from cognite_toolkit._cdf_tk.exceptions import AmbiguousResourceFileError


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
    def test_replace_string_number(self):
        source_yaml = """text: {{ my_text }}
bool: {{ my_bool }}
integer: {{ my_integer }}
float: {{ my_float }}
digit_string: {{ my_digit_string }}
packed_string: "{{ my_packed_string }}"
"""
        variables = {
            "my_text": "some text",
            "my_bool": True,
            "my_integer": 123,
            "my_float": 123.456,
            "my_digit_string": "123",
            "my_packed_string": "456",
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
        for key, value in variables.items():
            name = key.removeprefix("my_")
            assert loaded[name] == value
