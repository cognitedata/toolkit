import re
from dataclasses import dataclass


class InvalidJSONPathError(ValueError):
    pass


@dataclass(frozen=True)
class JSONPath:
    path: str

    def __post_init__(self) -> None:
        self._validate_path()

    def _validate_path(self) -> None:
        if not self.path:
            raise InvalidJSONPathError("JSON path cannot be empty")

        if self.path[0] == ".":
            raise InvalidJSONPathError(f"JSON path cannot start with a dot: '{self.path}'")

        if self.path[-1] == ".":
            raise InvalidJSONPathError(f"JSON path cannot end with a dot: '{self.path}'")

        if ".." in self.path:
            raise InvalidJSONPathError(f"JSON path contains consecutive dots (empty segment): '{self.path}'")

        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.{1}[a-zA-Z_][a-zA-Z0-9_]*|\[\d+\]|\['[^']*'\]|\[\"[^\"]*\"\])*$"

        if not re.match(pattern, self.path):
            raise InvalidJSONPathError(
                f"JSON path contains invalid syntax or characters: '{self.path}'. "
                f"Valid format: start with identifier, then use .field or [0] or ['key'] notation"
            )

    def __str__(self) -> str:
        return self.path
