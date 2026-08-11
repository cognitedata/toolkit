from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class RegExpSubstitutionContext:
    pattern: str
    replacement: str

    def __post_init__(self) -> None:
        if not self.pattern:
            raise ValueError("pattern cannot be empty")
        if not self.replacement:
            raise ValueError("replacement cannot be empty")


class RegExpSubstitutionContextBuilder:
    def __init__(self) -> None:
        self._pattern: str | None = None
        self._replacement: str | None = None

    def with_pattern(self, pattern: str) -> "RegExpSubstitutionContextBuilder":
        self._pattern = pattern
        return self

    def with_replacement(self, replacement: str) -> "RegExpSubstitutionContextBuilder":
        self._replacement = replacement
        return self

    def build(self) -> RegExpSubstitutionContext:
        if self._pattern is None:
            raise ValueError("pattern must be set before building")
        if self._replacement is None:
            raise ValueError("replacement must be set before building")
        return RegExpSubstitutionContext(pattern=self._pattern, replacement=self._replacement)


class RegExpSubstitutionRuleDefinition(RuleDefinition[RegExpSubstitutionContext]):
    def type(self) -> RuleType:
        return RuleType.REGEX_SUBSTITUTION

    def deserialize_context(self, payload: dict[str, Any]) -> RegExpSubstitutionContext:
        if "pattern" not in payload:
            raise ValueError("RegExpSubstitution rule payload must contain 'pattern' key")
        if "replacement" not in payload:
            raise ValueError("RegExpSubstitution rule payload must contain 'replacement' key")

        pattern = payload["pattern"]
        replacement = payload["replacement"]

        if not isinstance(pattern, str):
            raise ValueError("'pattern' must be a string")
        if not isinstance(replacement, str):
            raise ValueError("'replacement' must be a string")

        return RegExpSubstitutionContext(pattern=pattern, replacement=replacement)

    def create_kuiper_macro(self, context: RegExpSubstitutionContext) -> Macro:
        var_name = generate_variable_name()
        return Macro(
            definition=f"({var_name}) => {var_name}.map(value => regex_replace(value, '{context.pattern}', '{context.replacement}'))"
        )
