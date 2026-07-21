from dataclasses import dataclass
from enum import Enum
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


class CaseStrategy(str, Enum):
    UPPERCASE = "UPPERCASE"
    LOWERCASE = "LOWERCASE"


@dataclass(frozen=True)
class CaseTransformationContext:
    strategy: CaseStrategy

    def __post_init__(self) -> None:
        if not self.strategy:
            raise ValueError("strategy cannot be empty")


class CaseTransformationContextBuilder:
    def __init__(self) -> None:
        self._strategy: CaseStrategy | None = None

    def with_strategy(self, strategy: CaseStrategy) -> "CaseTransformationContextBuilder":
        self._strategy = strategy
        return self

    def build(self) -> CaseTransformationContext:
        if self._strategy is None:
            raise ValueError("strategy must be set before building")
        return CaseTransformationContext(strategy=self._strategy)


class CaseTransformationRuleDefinition(RuleDefinition[CaseTransformationContext]):
    def type(self) -> RuleType:
        return RuleType.CASE_TRANSFORMATION

    def deserialize_context(self, payload: dict[str, Any]) -> CaseTransformationContext:
        if "strategy" not in payload:
            raise ValueError("CaseTransformation rule payload must contain 'strategy' key")

        strategy_value = payload["strategy"]

        if not isinstance(strategy_value, str):
            raise ValueError("'strategy' must be a string")

        try:
            strategy = CaseStrategy(strategy_value)
        except ValueError:
            valid_strategies = ", ".join([s.value for s in CaseStrategy])
            raise ValueError(f"'strategy' must be one of: {valid_strategies}, got '{strategy_value}'")

        return CaseTransformationContext(strategy=strategy)

    def create_kuiper_macro(self, context: CaseTransformationContext) -> Macro:
        var_name = generate_variable_name()
        match context.strategy:
            case CaseStrategy.UPPERCASE:
                expression = f"({var_name}) => {var_name}.map(value => upper(value))"
            case CaseStrategy.LOWERCASE:
                expression = f"({var_name}) => {var_name}.map(value => lower(value))"

        return Macro(definition=expression)
