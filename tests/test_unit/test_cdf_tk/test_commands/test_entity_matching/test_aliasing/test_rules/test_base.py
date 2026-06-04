from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import (
    RuleType,
)


class TestRuleType:
    def test_enum_value_is_correct(self) -> None:
        assert RuleType.CHARACTER_SUBSTITUTION == "character_substitution"
