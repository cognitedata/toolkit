from dataclasses import dataclass
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


@dataclass(frozen=True)
class PrefixSuffixContext:
    prefix: str | None
    suffix: str | None

    def __post_init__(self) -> None:
        if not self.prefix and not self.suffix:
            raise ValueError("At least one of prefix or suffix must be provided and non-empty")


class PrefixSuffixRuleDefinition(RuleDefinition[PrefixSuffixContext]):
    def type(self) -> RuleType:
        return RuleType.PREFIX_SUFFIX

    def deserialize_context(self, payload: dict[str, Any]) -> PrefixSuffixContext:
        prefix = payload.get("prefix")
        suffix = payload.get("suffix")

        if prefix is not None and not isinstance(prefix, str):
            raise ValueError("'prefix' must be a string")
        if suffix is not None and not isinstance(suffix, str):
            raise ValueError("'suffix' must be a string")

        return PrefixSuffixContext(prefix=prefix, suffix=suffix)

    def create_kuiper_macro(self, context: PrefixSuffixContext) -> Macro:
        var_name = generate_variable_name()
        concat_args = []
        if context.prefix:
            concat_args.append(f'"{context.prefix}"')
        concat_args.append("value")
        if context.suffix:
            concat_args.append(f'"{context.suffix}"')

        args_str = ", ".join(concat_args)
        expression = f"({var_name}) => {var_name}.map(value => concat({args_str}))"

        return Macro(definition=expression)
