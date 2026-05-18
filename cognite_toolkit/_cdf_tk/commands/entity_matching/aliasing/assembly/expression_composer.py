from abc import ABC, abstractmethod

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro, MacroCallSignature, generate_macro_name
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro_variable_generator import generate_variable_name


class ExpressionComposer(ABC):
    @abstractmethod
    def compose(self, macros: list[Macro]) -> str:
        pass


class DefaultExpressionComposer(ExpressionComposer):
    def __init__(self, config: AliasingCompositionConfig | None = None) -> None:
        self._config = config or AliasingCompositionConfig()

    def compose(self, macros: list[Macro]) -> str:
        if not macros or len(macros) == 0:
            return self._build_empty_expression()

        macro_call_signatures = {}
        for macro in macros:
            macro_name = generate_macro_name()
            call_sig = MacroCallSignature(macro_name)
            macro_call_signatures[call_sig] = macro.as_string(call_sig)

        macro_definitions = " ".join(macro_call_signatures.values())
        composite_macro = self._build_composite_macro(macro_call_signatures)
        keys_path = str(self._config.keys_path)
        if self._config.output_projection is None:
            raise ValueError("output_projection cannot be None")
        projection_object = self._config.output_projection.generate_projection_object(
            "entity", "composite_aliases(entity.keys)"
        )

        return f"{macro_definitions} {composite_macro} {keys_path}.map(entity => {projection_object})"

    def _build_composite_macro(self, macro_call_signatures: dict[MacroCallSignature, str]) -> str:
        var_name = generate_variable_name()
        macro_calls = [call_sig.for_input(var_name) for call_sig in macro_call_signatures.keys()]
        invocations = ", ".join(macro_calls)

        lambda_definition = f"({var_name}) => [{invocations}].flatmap(all_aliases => all_aliases.flatmap(alias => alias)).distinct_by(alias => alias)"
        composite_macro = Macro(definition=lambda_definition)
        composite_call_sig = MacroCallSignature("composite_aliases")

        return composite_macro.as_string(composite_call_sig)

    def _build_empty_expression(self) -> str:
        keys_path = str(self._config.keys_path)
        empty_aliases_expr = "[]"
        if self._config.output_projection is None:
            raise ValueError("output_projection cannot be None")
        projection_object = self._config.output_projection.generate_projection_object("entity", empty_aliases_expr)
        return f"{keys_path}.map(entity => {projection_object})"
