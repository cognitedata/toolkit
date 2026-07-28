from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar

from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro


class RuleType(str, Enum):
    CHARACTER_SUBSTITUTION = "character_substitution"
    REGEX_SUBSTITUTION = "regex_substitution"
    PREFIX_SUFFIX = "prefix_suffix"
    CASE_TRANSFORMATION = "case_transformation"
    VALUE_EXPANSION = "value_expansion"
    LEADING_ZERO_NORMALIZATION = "leading_zero_normalization"
    COMPOSITE = "composite"


RuleContext = TypeVar("RuleContext")


class RuleDefinition(ABC, Generic[RuleContext]):
    @abstractmethod
    def type(self) -> RuleType:
        pass

    @abstractmethod
    def deserialize_context(self, payload: dict[str, Any]) -> RuleContext:
        pass

    @abstractmethod
    def create_kuiper_macro(self, context: RuleContext) -> Macro:
        pass


@dataclass(frozen=True)
class RuleName:
    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Rule name cannot be empty")


@dataclass(frozen=True)
class RuleDescription:
    description: str

    def __post_init__(self) -> None:
        if not self.description:
            raise ValueError("Rule description cannot be empty")


class Rule:
    def __init__(self, name: RuleName, description: RuleDescription, rule_definition: RuleDefinition[Any]) -> None:
        self.name: RuleName = name
        self.description: RuleDescription = description
        self.rule_definition: RuleDefinition[Any] = rule_definition

    @staticmethod
    def from_rule_definition(
        name: RuleName, description: RuleDescription, rule_definition: RuleDefinition[Any]
    ) -> "Rule":
        return Rule(name, description, rule_definition)

    def __repr__(self) -> str:
        return f"Rule(name={self.name}, description={self.description}, rule_definition={self.rule_definition})"

    def create_kuiper_macro(self, context: Any) -> Macro:
        return self.rule_definition.create_kuiper_macro(context)
