from dataclasses import dataclass
from itertools import product
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class ValueExpansionContext:
    expansions: dict[str, list[str]]

    def __post_init__(self) -> None:
        if not self.expansions:
            raise ValueError("expansions dictionary cannot be empty")
        for abbreviation, expansions_list in self.expansions.items():
            if not abbreviation:
                raise ValueError("abbreviation key cannot be empty")
            if not isinstance(expansions_list, list):
                raise ValueError("expansion values must be lists")
            if not expansions_list:
                raise ValueError("expansion list cannot be empty")
            for expansion in expansions_list:
                if not expansion:
                    raise ValueError("expansion value cannot be empty")


class ValueExpansionRuleDefinition(RuleDefinition[ValueExpansionContext]):
    def type(self) -> RuleType:
        return RuleType.VALUE_EXPANSION

    def deserialize_context(self, payload: dict[str, Any]) -> ValueExpansionContext:
        if "expansions" not in payload:
            raise ValueError("ValueExpansion rule payload must contain 'expansions' key")
        expansions = payload["expansions"]
        if not isinstance(expansions, dict):
            raise ValueError("'expansions' must be a dictionary mapping strings to lists of strings")
        for key, value in expansions.items():
            if not isinstance(value, list):
                raise ValueError(f"expansion for '{key}' must be a list, got {type(value).__name__}")
        return ValueExpansionContext(expansions=expansions)

    def create_kuiper_macro(self, context: ValueExpansionContext) -> Macro:
        var_name = generate_variable_name()
        abbreviations = sorted(context.expansions.keys())
        replacements_lists = [context.expansions[abbrev] for abbrev in abbreviations]

        combinations = list(product(*replacements_lists))

        if not combinations:
            expression = f"({var_name}) => {var_name}"
            return Macro(definition=expression)

        case_expressions = []
        for combo in combinations:
            expr = "value"
            for abbrev, replacement in zip(abbreviations, combo):
                expr += f'.replace("{abbrev}", "{replacement}")'
            case_expressions.append(expr)

        replacement_chain = ", ".join(case_expressions)

        expression = f"({var_name}) => {var_name}.flatmap(value => [{replacement_chain}])"

        return Macro(definition=expression)
