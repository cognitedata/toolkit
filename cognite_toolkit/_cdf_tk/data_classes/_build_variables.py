from __future__ import annotations

import re
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, SupportsIndex, overload

from ._module_directories import ModuleLocation


@dataclass(frozen=True)
class BuildVariable:
    """This is an internal representation of a  build variable in a config.[env].file

    Args:
        key: The name of the variable.
        value: The value of the variable.
        is_selected: Whether the variable is selected by the user through Environment.selected
        location: The location for the variable which is used to determine the module(s) it belongs to

    """

    key: str
    value: str | int | float | bool | tuple[str | int | float | bool]
    is_selected: bool
    location: Path

    @property
    def value_variable(self) -> str | int | float | bool | list[str | int | float | bool]:
        """Returns the value of the variable as a variable."""
        if isinstance(self.value, tuple):
            # Convert the tuple back to a list to make it JSON serializable
            return list(self.value)
        else:
            return self.value

    def dump(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value_variable,
            "is_selected": self.is_selected,
            "location": self.location.as_posix(),
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> BuildVariable:
        if isinstance(data["value"], list):
            # Convert the list to a tuple to make it hashable
            value = tuple(data["value"])
        else:
            value = data["value"]
        return cls(data["key"], value, data["is_selected"], Path(data["location"]))


class BuildVariables(tuple, Sequence[BuildVariable]):
    """This is an internal representation of the build variables in a config.[env].file

    The motivation for this class is to provide helper functions for the user to interact with the build variables.
    """

    # Subclassing tuple to make the class immutable. BuildVariables is expected to be initialized and
    # then used as a read-only object.
    def __new__(cls, collection: Collection[BuildVariable]) -> BuildVariables:
        # Need to override __new__ to as we are subclassing a tuple:
        #   https://stackoverflow.com/questions/1565374/subclassing-tuple-with-multiple-init-arguments
        return super().__new__(cls, tuple(collection))

    def __init__(self, collection: Collection[BuildVariable]) -> None: ...

    @cached_property
    def selected(self) -> BuildVariables:
        return BuildVariables([variable for variable in self if variable.is_selected])

    @classmethod
    def load_raw(
        cls,
        raw_variable: dict[str, Any],
        available_modules: set[Path],
        selected_modules: set[Path],
    ) -> BuildVariables:
        """Loads the variables from the user input."""
        variables = []
        to_check: list[tuple[Path, dict[str, Any]]] = [(Path(""), raw_variable)]
        while to_check:
            path, subdict = to_check.pop()
            for key, value in subdict.items():
                subpath = path / key
                if subpath in available_modules and isinstance(value, dict):
                    to_check.append((subpath, value))
                elif isinstance(value, dict):
                    # Remove this check to support variables with dictionary values.
                    continue
                else:
                    hashable_values = tuple(value) if isinstance(value, list) else value
                    variables.append(BuildVariable(key, hashable_values, path in selected_modules, path))

        return cls(variables)

    @classmethod
    def load(cls, data: list[dict[str, Any]]) -> BuildVariables:
        """Loads the variables from a dictionary."""
        return cls([BuildVariable.load(variable) for variable in data])

    def get_module_variables(self, module: ModuleLocation) -> BuildVariables:
        """Gets the variables for a specific module."""
        return BuildVariables(
            [
                variable
                for variable in self
                if variable.location == module.relative_path or variable.location in module.parent_relative_paths
            ]
        )

    def replace(self, content: str, file_suffix: str = ".yaml") -> str:
        for variable in self:
            replace = variable.value_variable
            _core_patter = rf"{{{{\s*{variable.key}\s*}}}}"
            if file_suffix in {".yaml", ".yml", ".json"}:
                # Preserve data types
                if isinstance(replace, str) and (replace.isdigit() or replace.endswith(":")):
                    replace = f'"{replace}"'
                elif replace is None:
                    replace = "null"
                content = re.sub(rf"'{_core_patter}'|{_core_patter}|" + rf'"{_core_patter}"', str(replace), content)
            else:
                content = re.sub(_core_patter, str(replace), content)

        return content

    # Implemented to get correct type hints
    def __iter__(self) -> Iterator[BuildVariable]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> BuildVariable: ...

    @overload
    def __getitem__(self, index: slice) -> BuildVariables: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> BuildVariable | BuildVariables:
        if isinstance(index, slice):
            return BuildVariables(super().__getitem__(index))
        return super().__getitem__(index)

    def dump(self) -> list[dict[str, Any]]:
        return [variable.dump() for variable in self]
