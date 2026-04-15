from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable, RelativeDirPath
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._lineage import ResourceLineageItem


@pytest.fixture(scope="session")
def relative_path_adapter() -> TypeAdapter[RelativeDirPath]:
    return TypeAdapter(RelativeDirPath)


def _create_variables(raw: dict[str, str | bool | int | float | list[str | bool | int | float]]) -> list[BuildVariable]:
    """Helper to create BuildVariable list from a dict of name->value."""
    return [BuildVariable(id=Path(f"modules/{name}"), value=value, is_selected=True) for name, value in raw.items()]


class TestRelativeDirPath:
    @pytest.mark.parametrize(
        "input_path, is_relative, error",
        [
            pytest.param("org/modules", False, "is not a relative path", id="Relative path without leading dot"),
            pytest.param("org/modules", True, "", id="Relative path with leading dot"),
            pytest.param("org/file.yaml", True, "is not a directory", id="Path with file suffix"),
        ],
    )
    def test_relative_dir_path(
        self,
        input_path: str,
        is_relative: bool,
        error: str,
        tmp_path: Path,
        relative_path_adapter: TypeAdapter[RelativeDirPath],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        test_path = tmp_path / input_path
        if test_path.suffix:  # If the path has a suffix, create it as a file
            test_path.parent.mkdir(parents=True, exist_ok=True)
            test_path.touch()
        else:
            test_path.mkdir(parents=True, exist_ok=True)
        if is_relative:
            test_path = test_path.relative_to(tmp_path)

        if error:
            with pytest.raises(ValueError) as exc_info:
                relative_path_adapter.validate_python(test_path)
            assert error in str(exc_info.value)
        else:
            result = relative_path_adapter.validate_python(test_path)
            assert result == Path(input_path)


class TestBuildVariable:
    def test_substitute_preserve_data_type(self) -> None:
        source_yaml = """text: {{ my_text }}
bool: {{ my_bool }}
integer: {{ my_integer }}
float: {{ my_float }}
digit_string: {{ my_digit_string }}
quoted_string: "{{ my_quoted_string }}"
single_quoted_string: '{{ my_single_quoted_string }}'
composite: 'some_prefix_{{ my_composite }}'
prefix_text: {{ my_prefix_text }}
suffix_text: {{ my_suffix_text }}
"""
        variables = _create_variables(
            {
                "my_text": "some text",
                "my_bool": True,
                "my_integer": 123,
                "my_float": 123.456,
                "my_digit_string": "123",
                "my_quoted_string": "456",
                "my_single_quoted_string": "789",
                "my_composite": "the suffix",
                "my_prefix_text": "prefix:",
                "my_suffix_text": ":suffix",
            },
        )

        result = BuildVariable.substitute(source_yaml, variables)

        loaded = yaml.safe_load(result)
        assert loaded == {
            "text": "some text",
            "bool": True,
            "integer": 123,
            "float": 123.456,
            "digit_string": "123",
            "quoted_string": "456",
            "single_quoted_string": "789",
            "composite": "some_prefix_the suffix",
            "prefix_text": "prefix:",
            "suffix_text": ":suffix",
        }

    def test_substitute_sql_not_preserve_type(self) -> None:
        source_sql = """dataset_id('{{dataset_external_id}}')"""
        variables = _create_variables(
            {
                "dataset_external_id": "ds_external_id",
            },
        )

        result = BuildVariable.substitute(source_sql, variables, ".sql")

        assert result == "dataset_id('ds_external_id')"

    def test_substitute_sql_list(self) -> None:
        """Test that lists with mixed types in SQL files are formatted correctly."""
        source_sql = """SELECT * FROM table WHERE column IN {{ my_list }}"""
        variables = _create_variables(
            {
                "my_list": ["A", 123, True],
            },
        )

        result = BuildVariable.substitute(source_sql, variables, ".sql")

        assert result == "SELECT * FROM table WHERE column IN ('A', 123, True)"

    def test_substitute_yaml_preserve_double_quotes(self) -> None:
        source_yaml = """externalId: some_id
name: Some Transformation
destination:
  type: nodes
  view:
    space: cdf_cdm
    externalId: CogniteTimeSeries
    version: v1
  instanceSpace: my_instance_space
query: >-
  select "fpso_{{location_id}}" as externalId, "{{location_ID}}" as uid, "{{location_ID}}" as description
"""
        variables = _create_variables(
            {
                "location_id": "uny",
                "location_ID": "UNY",
            },
        )

        result = BuildVariable.substitute(source_yaml, variables, ".yaml")

        loaded = yaml.safe_load(result)

        assert loaded["query"] == 'select "fpso_uny" as externalId, "UNY" as uid, "UNY" as description'

    @pytest.mark.parametrize(
        "yaml_content, expected",
        [
            pytest.param(
                """instanceSpaces:
{{ list_one }}
{{ list_two }}
""",
                {"instanceSpaces": ["a", "b", "c", "d", "e"]},
                id="Outer list",
            ),
            pytest.param(
                """rules:
  instanceSpace:
    {{ list_one }}
    {{ list_two }}
             """,
                {"rules": {"instanceSpace": ["a", "b", "c", "d", "e"]}},
                id="Nested list",
            ),
        ],
    )
    def test_substitute_concat_lists(self, yaml_content: str, expected: dict[str, Any]) -> None:
        variables = _create_variables({"list_one": ["a", "b", "c"], "list_two": ["d", "e"]})

        result = BuildVariable.substitute(yaml_content, variables, ".yaml")
        loaded = yaml.safe_load(result)

        assert loaded == expected

    def test_format_list_as_sql_tuple_empty(self) -> None:
        """Test that empty lists become empty SQL tuples."""
        result = BuildVariable._format_list_as_sql_tuple([])

        assert result == "()"

    def test_format_list_as_sql_tuple_with_strings(self) -> None:
        """Test that strings are properly quoted in SQL tuples."""
        result = BuildVariable._format_list_as_sql_tuple(["a", "b", "c"])

        assert result == "('a', 'b', 'c')"

    def test_format_list_as_sql_tuple_with_mixed_types(self) -> None:
        """Test that mixed types are properly formatted in SQL tuples."""
        result = BuildVariable._format_list_as_sql_tuple(["A", 123, True])

        assert result == "('A', 123, True)"

    def test_get_pattern_replace_pair_yaml_digit_string(self) -> None:
        """Test that digit strings are quoted in YAML."""
        variable = BuildVariable(id=Path("modules/my_var"), value="123", is_selected=True)

        _, replace = variable.get_pattern_replace_pair(".yaml")

        assert replace == '"123"'

    def test_get_pattern_replace_pair_yaml_colon_suffix(self) -> None:
        """Test that strings ending with colon are quoted in YAML."""
        variable = BuildVariable(id=Path("modules/my_var"), value="prefix:", is_selected=True)

        _, replace = variable.get_pattern_replace_pair(".yaml")

        assert replace == '"prefix:"'

    def test_get_pattern_replace_pair_sql_list(self) -> None:
        """Test that lists are formatted as SQL tuples in .sql files."""
        variable = BuildVariable(id=Path("modules/my_list"), value=["X", "Y", "Z"], is_selected=True)

        _, replace = variable.get_pattern_replace_pair(".sql")

        assert replace == "('X', 'Y', 'Z')"

    def test_get_pattern_replace_pair_unsupported_suffix(self) -> None:
        """Test that unsupported file suffixes raise NotImplementedError."""
        variable = BuildVariable(id=Path("modules/my_var"), value="test", is_selected=True)

        with pytest.raises(NotImplementedError, match=r"'.txt' is not supported"):
            variable.get_pattern_replace_pair(".txt")  # type: ignore[arg-type]


class TestBuildLinage:
    def test_deserialize_resource_lineage(self, tmp_path: Path) -> None:
        source_file = tmp_path / "file1.txt"
        built_file = tmp_path / "file2.txt"
        source_file.touch()
        built_file.touch()
        data = {
            "sourceFile": source_file.as_posix(),
            "sourceHash": "123",
            "type": {
                "resource_folder": "files",
                "kind": "FileMetadata",
            },
            "builtFile": built_file.as_posix(),
            "identifier": {"externalId": "some_id"},
        }
        linage = ResourceLineageItem.model_validate(data)
        assert isinstance(linage.identifier, ExternalId)
