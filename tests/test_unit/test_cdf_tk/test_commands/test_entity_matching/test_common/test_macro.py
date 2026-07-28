import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro, MacroCallSignature


class TestMacroCallSignature:
    def test_when_valid_macro_name_then_creation_succeeds(self) -> None:
        sig = MacroCallSignature("my_macro")
        assert sig.macro_name == "my_macro"

    def test_when_empty_macro_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Call signature cannot be empty"):
            MacroCallSignature("")

    def test_when_none_macro_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Call signature cannot be empty"):
            MacroCallSignature(None)  # type: ignore[arg-type]

    def test_when_calling_for_input_then_generates_correct_signature(self) -> None:
        sig = MacroCallSignature("my_macro")
        result = sig.for_input("my_input")
        assert result == "my_macro(my_input)"


class TestMacro:
    def test_when_valid_definition_then_creation_succeeds(self) -> None:
        lambda_def = "(s) => s.replace('a', 'b')"
        macro = Macro(definition=lambda_def)
        assert macro.definition == lambda_def

    def test_when_empty_definition_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Macro definition cannot be empty"):
            Macro(definition="")

    def test_when_as_string_then_formats_correctly(self) -> None:
        lambda_def = "(s) => s.replace('a', 'b')"
        macro = Macro(definition=lambda_def)
        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert result == "#test_macro := (s) => s.replace('a', 'b');"

    def test_when_as_string_with_different_signatures_then_formats_correctly(self) -> None:
        lambda_def = "(v) => v.map(x => x)"
        macro = Macro(definition=lambda_def)

        sig1 = MacroCallSignature("macro_one")
        result1 = macro.as_string(sig1)
        assert result1 == "#macro_one := (v) => v.map(x => x);"

        sig2 = MacroCallSignature("macro_two")
        result2 = macro.as_string(sig2)
        assert result2 == "#macro_two := (v) => v.map(x => x);"
