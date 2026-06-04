from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class ResolvedRuleSpec:
    definition: RuleDefinition[Any]
    payload: dict[str, Any]


@dataclass(frozen=True)
class CompositeRuleContext:
    rules: list[ResolvedRuleSpec]

    def __post_init__(self) -> None:
        if not self.rules:
            raise ValueError("At least one rule must be specified in composite rule")
        for rule_spec in self.rules:
            if not isinstance(rule_spec, ResolvedRuleSpec):
                raise ValueError("All items in rules list must be ResolvedRuleSpec instances")


class CompositeRuleDefinition(RuleDefinition[CompositeRuleContext]):
    def type(self) -> RuleType:
        return RuleType.COMPOSITE

    def deserialize_context(self, payload: dict[str, Any]) -> CompositeRuleContext:
        if "rules" not in payload:
            raise ValueError("Composite rule payload must contain 'rules' key")

        rules_list = payload["rules"]
        if not isinstance(rules_list, list):
            raise ValueError("'rules' must be a list")

        if not rules_list:
            raise ValueError("'rules' list cannot be empty")

        for item in rules_list:
            if not isinstance(item, ResolvedRuleSpec):
                raise ValueError("All items in rules must be ResolvedRuleSpec instances")

        return CompositeRuleContext(rules=rules_list)

    def create_kuiper_macro(self, context: CompositeRuleContext) -> Macro:
        if not context.rules:
            raise ValueError("No rules to compose")

        sub_rule_macros: list[Macro] = []
        for resolved_spec in context.rules:
            definition = resolved_spec.definition
            sub_context = definition.deserialize_context(resolved_spec.payload)
            sub_macro = definition.create_kuiper_macro(sub_context)
            sub_rule_macros.append(sub_macro)

        var_name = generate_variable_name()

        if len(sub_rule_macros) == 1:
            inner_macro = sub_rule_macros[0]
            expression = f"({var_name}) => ({inner_macro.definition})({var_name})"
        else:
            composition_chain = var_name
            for macro in reversed(sub_rule_macros):
                composition_chain = f"({macro.definition})({composition_chain})"

            expression = f"({var_name}) => {composition_chain}"

        return Macro(definition=expression)
