from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import (
    LocalRuleDefinitionRegistry,
    RuleDefinitionNotFoundError,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType


class TestLocalRuleDefinitionRegistry:
    def test_when_definition_exists_then_returns_definition(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        definitions: dict[RuleType, RuleDefinition] = {RuleType.CHARACTER_SUBSTITUTION: mock_definition}
        registry = LocalRuleDefinitionRegistry(definitions)

        result = registry.get_definition_or_throw(RuleType.CHARACTER_SUBSTITUTION)

        assert result == mock_definition

    def test_when_rule_type_not_registered_then_raises_error(self) -> None:
        definitions: dict[RuleType, RuleDefinition] = {}
        registry = LocalRuleDefinitionRegistry(definitions)

        with pytest.raises(RuleDefinitionNotFoundError, match="character_substitution"):
            registry.get_definition_or_throw(RuleType.CHARACTER_SUBSTITUTION)
