import uuid


def generate_macro_name() -> str:
    return f"m_{uuid.uuid4().hex}"


class MacroCallSignature:
    def __init__(self, macro_name: str):
        if not macro_name:
            raise ValueError("Call signature cannot be empty")

        self.macro_name = macro_name

    def for_input(self, input_name: str) -> str:
        return f"{self.macro_name}({input_name})"

    def __repr__(self) -> str:
        return f"MacroCallSignature(macro_name={self.macro_name})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MacroCallSignature):
            return False
        return self.macro_name == other.macro_name

    def __hash__(self) -> int:
        return hash(self.macro_name)


class Macro:
    def __init__(self, definition: str) -> None:
        if not definition:
            raise ValueError("Macro definition cannot be empty")
        self.definition = definition

    def as_string(self, call_signature: MacroCallSignature) -> str:
        return f"#{call_signature.macro_name} := {self.definition};"

    def __repr__(self) -> str:
        return f"Macro(definition={self.definition})"
