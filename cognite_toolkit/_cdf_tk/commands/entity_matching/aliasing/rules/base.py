from abc import ABC, abstractmethod
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

