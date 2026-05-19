from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.errors import InvalidRuleFormatError, YamlReadError
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.yaml_rules_reader import (
    RulesFileContent,
    YamlRulesReader,
)


@pytest.fixture
def reader() -> YamlRulesReader:
    return YamlRulesReader()


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent / "test_data"


class TestYamlRulesReader:
    def test_read_valid_rules(self, reader: YamlRulesReader, data_dir: Path) -> None:
        from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
            AliasingRule,
        )

        file_path = data_dir / "valid_single_rule.yaml"
        result = reader.read_file(str(file_path))

        expected_rule = AliasingRule(
            name="replace_p_with_k",
            rule_type="character_substitution",
            description="Replace P with K",
            payload={"replacements": {"P": "K"}},
        )

        assert isinstance(result, RulesFileContent)
        assert len(result.rules) == 1
        assert result.rules[0] == expected_rule
        assert result.key_path == 'properties.cdf_cdm["CogniteAsset/v1"].name'
        assert result.workflow_id == "entity_matching_aliasing"
        assert result.description == "Entity matching aliasing workflow"

    def test_rule_level_error_with_actionable_message(self, reader: YamlRulesReader, data_dir: Path) -> None:
        file_path = data_dir / "error_rule_index_context.yaml"
        with pytest.raises(InvalidRuleFormatError) as exc_info:
            reader.read_file(str(file_path))

        error = exc_info.value
        error_str = str(error)

        assert "Rule index: 1" in error_str
        assert "invalid_rule" in error_str
        assert "payload" in error_str.lower()

    def test_file_not_found_error(self, reader: YamlRulesReader) -> None:
        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file("/nonexistent/path/rules.yaml")

        error = exc_info.value
        assert "File not found" in str(error)

    def test_fail_fast_stops_on_first_error(self, reader: YamlRulesReader, data_dir: Path) -> None:
        file_path = data_dir / "error_fail_fast.yaml"
        with pytest.raises(InvalidRuleFormatError) as exc_info:
            reader.read_file(str(file_path))

        error = exc_info.value
        error_str = str(error)

        assert "Rule index: 1" in error_str
        assert "Rule index: 2" not in error_str

    def test_empty_rules_error(self, reader: YamlRulesReader, data_dir: Path) -> None:
        file_path = data_dir / "empty_rules_list.yaml"
        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file(str(file_path))

        error = exc_info.value
        assert "empty" in str(error).lower()

    def test_missing_key_path_error(self, reader: YamlRulesReader, data_dir: Path, tmp_path: Path) -> None:
        yaml_file = tmp_path / "no_key_path.yaml"
        yaml_file.write_text(
            "rules:\n  - name: test\n    rule_type: character_substitution\n    description: Test\n    payload: {}\n"
        )

        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file(str(yaml_file))

        error = exc_info.value
        assert "key_path" in str(error).lower()

    def test_empty_key_path_error(self, reader: YamlRulesReader, data_dir: Path, tmp_path: Path) -> None:
        yaml_file = tmp_path / "empty_key_path.yaml"
        yaml_file.write_text(
            "key_path: ''\nrules:\n  - name: test\n    rule_type: character_substitution\n    description: Test\n    payload: {}\n"
        )

        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file(str(yaml_file))

        error = exc_info.value
        assert "empty" in str(error).lower()

    def test_read_with_custom_name_and_description(self, reader: YamlRulesReader, tmp_path: Path) -> None:

        yaml_file = tmp_path / "custom_workflow.yaml"
        yaml_file.write_text(
            "key_path: 'properties.name'\n"
            "workflow_id: 'my_custom_workflow'\n"
            "description: 'My custom workflow description'\n"
            "rules:\n"
            "  - name: test_rule\n"
            "    rule_type: character_substitution\n"
            "    description: Test Rule\n"
            "    payload:\n"
            "      replacements:\n"
            "        'A': 'B'\n"
        )

        result = reader.read_file(str(yaml_file))

        assert result.workflow_id == "my_custom_workflow"
        assert result.description == "My custom workflow description"
        assert result.key_path == "properties.name"
        assert len(result.rules) == 1

    def test_read_with_custom_name_only(self, reader: YamlRulesReader, tmp_path: Path) -> None:
        yaml_file = tmp_path / "custom_name_only.yaml"
        yaml_file.write_text(
            "key_path: 'properties.name'\n"
            "workflow_id: 'custom_workflow'\n"
            "rules:\n"
            "  - name: test_rule\n"
            "    rule_type: character_substitution\n"
            "    description: Test Rule\n"
            "    payload:\n"
            "      replacements:\n"
            "        'A': 'B'\n"
        )

        result = reader.read_file(str(yaml_file))

        assert result.workflow_id == "custom_workflow"
        assert result.description == "Entity matching aliasing workflow"

    def test_read_with_custom_description_only(self, reader: YamlRulesReader, tmp_path: Path) -> None:
        yaml_file = tmp_path / "custom_desc_only.yaml"
        yaml_file.write_text(
            "key_path: 'properties.name'\n"
            "description: 'Custom description'\n"
            "rules:\n"
            "  - name: test_rule\n"
            "    rule_type: character_substitution\n"
            "    description: Test Rule\n"
            "    payload:\n"
            "      replacements:\n"
            "        'A': 'B'\n"
        )

        result = reader.read_file(str(yaml_file))

        assert result.workflow_id == "entity_matching_aliasing"
        assert result.description == "Custom description"

    def test_invalid_name_type_error(self, reader: YamlRulesReader, tmp_path: Path) -> None:
        yaml_file = tmp_path / "invalid_name_type.yaml"
        yaml_file.write_text("key_path: 'properties.name'\nworkflow_id: 123\nrules: []\n")

        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file(str(yaml_file))

        error = exc_info.value
        assert "workflow_id" in str(error).lower()
        assert "string" in str(error).lower()

    def test_invalid_description_type_error(self, reader: YamlRulesReader, tmp_path: Path) -> None:
        yaml_file = tmp_path / "invalid_desc_type.yaml"
        yaml_file.write_text("key_path: 'properties.name'\ndescription: true\nrules: []\n")

        with pytest.raises(YamlReadError) as exc_info:
            reader.read_file(str(yaml_file))

        error = exc_info.value
        assert "description" in str(error).lower()
        assert "string" in str(error).lower()
