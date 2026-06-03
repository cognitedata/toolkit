from collections import Counter
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composer import ExpressionComposer
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import RuleDefinitionRegistry
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


@dataclass(frozen=True)
class AliasingRule:
    name: str
    rule_type: str
    description: str
    payload: dict[str, Any]


class AliasingKuiperBuilder(ABC):
    @abstractmethod
    def with_rule(self, rule: AliasingRule) -> "AliasingKuiperBuilder":
        pass

    @abstractmethod
    def build(self) -> AliasingKuiper:
        pass


class BuilderConstraintError(ToolkitValueError):
    pass


class EmptyRulesError(BuilderConstraintError):
    def __init__(self) -> None:
        super().__init__("At least one rule must be added before calling build()")


class DuplicateRuleNameError(BuilderConstraintError):
    def __init__(self, duplicate_names: set[str]) -> None:
        sorted_names = sorted(duplicate_names)
        message = f"Duplicate rule names found: {', '.join(sorted_names)}"
        super().__init__(message)
        self.duplicate_names = duplicate_names


class DefaultAliasingKuiperBuilder(AliasingKuiperBuilder):
    def __init__(self, registry: RuleDefinitionRegistry, composer: ExpressionComposer) -> None:
        self._registry = registry
        self._composer = composer
        self._rules: list[AliasingRule] = []

    def with_rule(self, rule: AliasingRule) -> "DefaultAliasingKuiperBuilder":
        self._rules.append(rule)
        return self

    def build(self) -> AliasingKuiper:
        resolved_rules = self._resolve_composite_rules(self._rules)

        self._validate_rules(resolved_rules)

        macros: list[Any] = []
        for rule in resolved_rules:
            rule_definition = self._registry.get_definition_or_throw(rule.rule_type)  # type: ignore[arg-type]
            context = rule_definition.deserialize_context(rule.payload)
            macro = rule_definition.create_kuiper_macro(context)
            macros.append(macro)

        expression = self._composer.compose(macros)
        return AliasingKuiper(expression=expression)

    def _resolve_composite_rules(self, rules: list[AliasingRule]) -> list[AliasingRule]:
        resolved: list[AliasingRule] = []

        for rule in rules:
            if rule.rule_type != "composite":
                resolved.append(rule)
                continue

            payload = rule.payload
            if "rules" not in payload:
                raise ValueError(f"Composite rule '{rule.name}' missing 'rules' key in payload")

            rules_list = payload["rules"]
            if not isinstance(rules_list, list):
                raise ValueError(f"Composite rule '{rule.name}' has invalid 'rules' format")

            if not rules_list:
                raise ValueError(f"Composite rule '{rule.name}' has empty rules list")

            expanded_sub_rules = []

            for idx, sub_spec in enumerate(rules_list):
                if not isinstance(sub_spec, dict):
                    raise ValueError(f"Sub-rule specification {idx} in composite '{rule.name}' must be a dict")

                if "rule_type" not in sub_spec or "payload" not in sub_spec:
                    raise ValueError(
                        f"Sub-rule specification {idx} in composite '{rule.name}' must have 'rule_type' and 'payload'"
                    )

                definition = self._registry.get_definition_or_throw(sub_spec["rule_type"])

                sub_rule_name = f"{rule.name}_sub_{idx}"
                sub_rule = AliasingRule(
                    name=sub_rule_name,
                    rule_type=sub_spec["rule_type"],
                    description=f"Sub-rule of composite '{rule.name}'",
                    payload=sub_spec["payload"],
                )
                expanded_sub_rules.append(sub_rule)

            for sub_rule in expanded_sub_rules:
                resolved.extend(self._resolve_composite_rules([sub_rule]))

        return resolved

    def _validate_rules(self, rules: list[AliasingRule]) -> None:
        if not rules:
            raise EmptyRulesError()

        name_counts = Counter(rule.name for rule in rules)
        duplicates = {name for name, count in name_counts.items() if count > 1}
        if duplicates:
            raise DuplicateRuleNameError(duplicates)


