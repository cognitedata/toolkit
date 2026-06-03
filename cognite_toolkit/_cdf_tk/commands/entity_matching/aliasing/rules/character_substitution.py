from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class CharacterSubstitutionContext:
    replacements: dict[str, str]

    def __post_init__(self) -> None:
        if not self.replacements:
            raise ValueError("replacements dictionary cannot be empty")
        for from_char, _ in self.replacements.items():
            if not from_char:
                raise ValueError("from_char cannot be empty")


class CharacterSubstitutionRuleDefinition(RuleDefinition[CharacterSubstitutionContext]):
    def type(self) -> RuleType:
        return RuleType.CHARACTER_SUBSTITUTION

    def deserialize_context(self, payload: dict[str, Any]) -> CharacterSubstitutionContext:
        if "replacements" not in payload:
            raise ValueError("CharacterSubstitution rule payload must contain 'replacements' key")
        replacements = payload["replacements"]
        if not isinstance(replacements, dict):
            raise ValueError("'replacements' must be a dictionary mapping strings to strings")
        return CharacterSubstitutionContext(replacements=replacements)

    def create_kuiper_macro(self, context: CharacterSubstitutionContext) -> Macro:
        var_name = generate_variable_name()
        replace_chain = "char"
        for from_char, to_char in context.replacements.items():
            replace_chain += f".replace('{from_char}', '{to_char}')"

        return Macro(definition=f"({var_name}) => {var_name}.map(char => {replace_chain})")
