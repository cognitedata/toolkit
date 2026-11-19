import re
from dataclasses import dataclass
from typing import Literal

SPECIAL_CHARACTERS = ":()=,"


@dataclass
class ParsedEntity:
    """Represents a parsed entity string."""

    prefix: str
    suffix: str
    properties: dict[str, str]

    def __str__(self) -> str:
        props_str = ""
        if self.properties:
            joined = ",".join(f"{k}={v}" for k, v in sorted(self.properties.items(), key=lambda x: x[0]))
            props_str = f"({joined})"
        if self.prefix:
            return f"{self.prefix}:{self.suffix}{props_str}"
        return f"{self.suffix}{props_str}"

    def __hash__(self) -> int:
        return hash(str(self))


class _EntityParser:
    """A parser for entity strings in the format 'prefix:suffix(prop1=val1,prop2=val2)'."""

    def __init__(self, entity_string: str):
        self.entity_string = entity_string.strip() if entity_string else ""
        self.pos = 0
        self.length = len(self.entity_string)

    def peek(self) -> str:
        if self.pos >= self.length:
            return ""
        return self.entity_string[self.pos]

    def advance(self) -> str:
        if self.pos >= self.length:
            return ""
        char = self.entity_string[self.pos]
        self.pos += 1
        return char

    def skip_whitespace(self) -> None:
        while self.pos < self.length and self.entity_string[self.pos].isspace():
            self.pos += 1

    def parse_identifier(self) -> str:
        start = self.pos
        while self.pos < self.length and self.entity_string[self.pos] not in SPECIAL_CHARACTERS:
            self.pos += 1
        return self.entity_string[start : self.pos].strip()

    def parse_property_value(self) -> str:
        start = self.pos
        paren_depth = 0

        while self.pos < self.length:
            char = self.entity_string[self.pos]
            if char == "(":
                paren_depth += 1
            elif char == ")":
                if paren_depth == 0:
                    break
                paren_depth -= 1
            elif char == "," and paren_depth == 0:
                break
            self.pos += 1

        return self.entity_string[start : self.pos].strip()

    def parse_properties(self) -> dict[str, str]:
        properties: dict[str, str] = {}

        if self.peek() == "(":
            self.advance()
        else:
            raise ValueError("expected `(`")

        self.skip_whitespace()

        while self.pos < self.length and self.peek() != ")":
            self.skip_whitespace()
            if self.peek() == ")":
                break

            prop_name = self.parse_identifier()
            if not prop_name:
                raise ValueError(f"Expected property name at position {self.pos}. Got {self.peek()!r}")
            self.skip_whitespace()

            if self.peek() != "=":
                raise ValueError(
                    f"Expected '=' after property name '{prop_name}' at position {self.pos}. Got {self.peek()!r}"
                )
            self.advance()

            self.skip_whitespace()
            prop_value = self.parse_property_value()
            properties[prop_name] = prop_value

            self.skip_whitespace()
            if self.peek() == ",":
                self.advance()

        if self.pos >= self.length:
            raise ValueError(f"Expected ')' to close properties at position {self.length}")

        if self.peek() == ")":
            self.advance()

        return properties

    def parse(self) -> ParsedEntity:
        if not self.entity_string:
            return ParsedEntity(prefix="", suffix="", properties={})
        if self.entity_string.strip() == ":":
            raise ValueError("Expected identifier at position 0")

        main_id = self.parse_identifier()
        prefix = ""
        suffix = ""

        if self.peek() == ":":
            self.advance()
            prefix = main_id
            suffix = self.parse_identifier()
            if not suffix:
                raise ValueError(f"Expected identifier after ':' at position {self.pos}")
        else:
            suffix = main_id

        self.skip_whitespace()
        properties: dict[str, str] = {}
        if self.peek() == "(":
            properties = self.parse_properties()

        self.skip_whitespace()
        if self.pos < self.length:
            raise ValueError(f"Unexpected characters after properties at position {self.pos}. Got {self.peek()!r}")

        return ParsedEntity(prefix, suffix, properties)


def parse_entity(entity_string: str) -> ParsedEntity:
    """Parse an entity string into its prefix, suffix, and properties."""
    return _EntityParser(entity_string).parse()


def parse_entities(entities_str: str, separator: Literal[","] = ",") -> list[ParsedEntity] | None:
    """Parse a comma-separated string of entities."""
    if not entities_str.strip():
        return None
    pattern = rf"{separator}(?![^()]*\))"
    parts = re.split(pattern, entities_str)
    return [parse_entity(part.strip()) for part in parts if part.strip()]
