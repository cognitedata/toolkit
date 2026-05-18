from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.errors import InvalidRuleFormatError, YamlReadError
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.yaml_rules_reader import YamlRulesReader


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
        rules = reader.read_file(str(file_path))

        expected_rule = AliasingRule(
            name="replace_p_with_k",
            rule_type="character_substitution",
            description="Replace P with K",
            payload={"replacements": {"P": "K"}},
        )

        assert len(rules) == 1
        assert rules[0] == expected_rule

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
