import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import InvalidJSONPathError, JSONPath


class TestJSONPath:
    def test_when_simple_field_then_creation_succeeds(self) -> None:
        path = JSONPath("field")
        assert path.path == "field"

    def test_when_nested_fields_with_dots_then_creation_succeeds(self) -> None:
        path = JSONPath("input.keys")
        assert path.path == "input.keys"

    def test_when_multiple_nested_fields_then_creation_succeeds(self) -> None:
        path = JSONPath("entity.data.value")
        assert path.path == "entity.data.value"

    def test_when_field_with_array_index_then_creation_succeeds(self) -> None:
        path = JSONPath("items[0]")
        assert path.path == "items[0]"

    def test_when_field_with_multiple_array_indices_then_creation_succeeds(self) -> None:
        path = JSONPath("data[0].items[1]")
        assert path.path == "data[0].items[1]"

    def test_when_field_with_string_key_single_quotes_then_creation_succeeds(self) -> None:
        path = JSONPath("data['key']")
        assert path.path == "data['key']"

    def test_when_field_with_string_key_double_quotes_then_creation_succeeds(self) -> None:
        path = JSONPath('data["key"]')
        assert path.path == 'data["key"]'

    def test_when_complex_path_with_mixed_notation_then_creation_succeeds(self) -> None:
        path = JSONPath("input.keys[0]['id']")
        assert path.path == "input.keys[0]['id']"

    def test_when_empty_path_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path cannot be empty"):
            JSONPath("")

    def test_when_none_path_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path cannot be empty"):
            JSONPath(None)  # type: ignore[arg-type]

    def test_when_path_starts_with_dot_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path cannot start with a dot"):
            JSONPath(".field")

    def test_when_path_ends_with_dot_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path cannot end with a dot"):
            JSONPath("field.")

    def test_when_path_has_consecutive_dots_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path contains consecutive dots"):
            JSONPath("field..nested")

    def test_when_path_starts_with_number_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path contains invalid syntax or characters"):
            JSONPath("0field")

    def test_when_path_has_invalid_characters_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path contains invalid syntax or characters"):
            JSONPath("field-name")

    def test_when_path_has_special_characters_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path contains invalid syntax or characters"):
            JSONPath("field@name")

    def test_when_path_has_unclosed_bracket_then_raises_invalid_json_path_error(self) -> None:
        with pytest.raises(InvalidJSONPathError, match="JSON path contains invalid syntax or characters"):
            JSONPath("field[0")

    def test_when_path_has_valid_complex_structure_then_creation_succeeds(self) -> None:
        path = JSONPath("data[0]['item'].value[1]")
        assert path.path == "data[0]['item'].value[1]"

    def test_when_str_called_then_returns_path_string(self) -> None:
        path = JSONPath("input.keys")
        assert str(path) == "input.keys"

    def test_when_two_identical_paths_then_are_equal(self) -> None:
        path1 = JSONPath("input.keys")
        path2 = JSONPath("input.keys")
        assert path1 == path2
