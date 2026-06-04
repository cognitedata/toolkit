from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class LeadingZeroNormalizationContext:
    target_length: int

    def __post_init__(self) -> None:
        if self.target_length < 0:
            raise ValueError("target_length cannot be negative")


class LeadingZeroNormalizationRuleDefinition(RuleDefinition[LeadingZeroNormalizationContext]):
    def type(self) -> RuleType:
        return RuleType.LEADING_ZERO_NORMALIZATION

    def deserialize_context(self, payload: dict[str, Any]) -> LeadingZeroNormalizationContext:
        if "target_length" not in payload:
            raise ValueError("LeadingZeroNormalization rule payload must contain 'target_length' key")

        target_length = payload["target_length"]

        if not isinstance(target_length, int):
            raise ValueError("'target_length' must be an integer")

        if target_length < 0:
            raise ValueError("'target_length' cannot be negative")

        return LeadingZeroNormalizationContext(target_length=target_length)

    def create_kuiper_macro(self, context: LeadingZeroNormalizationContext) -> Macro:
        var_name = generate_variable_name()
        target_length = context.target_length
        padding = "0" * target_length
        expression = (
            f"({var_name}) => {var_name}.map(value => "
            f"coalesce("
            f'regex_first_captures(value, "^(.*?)(\\\\d+)(.*?)$").if_value(captures => '
            f'concat(captures["1"], '
            f'substring(concat("{padding}", captures["2"]), '
            f'length(concat("{padding}", captures["2"])) - {target_length}), '
            f'captures["3"])), '
            f"value))"
        )

        return Macro(definition=expression)
