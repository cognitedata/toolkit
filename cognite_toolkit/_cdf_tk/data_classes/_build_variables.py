from __future__ import annotations

import re
import uuid
from collections import defaultdict
from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, SupportsIndex, overload

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags

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
    iteration: int | None = None

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
    def __new__(cls, collection: Collection[BuildVariable], source_path: Path | None = None) -> BuildVariables:
        # Need to override __new__ to as we are subclassing a tuple:
        #   https://stackoverflow.com/questions/1565374/subclassing-tuple-with-multiple-init-arguments
        return super().__new__(cls, tuple(collection))

    def __init__(self, collection: Collection[BuildVariable], source_path: Path | None = None) -> None:
        super().__init__()
        self.source_path = source_path

    @cached_property
    def selected(self) -> BuildVariables:
        return BuildVariables([variable for variable in self if variable.is_selected])

    @classmethod
    def load_raw(
        cls,
        raw_variable: dict[str, Any],
        available_modules: set[Path],
        selected_modules: set[Path] | None = None,
        source_path: Path | None = None,
    ) -> BuildVariables:
        """Loads the variables from the user input."""
        variables = []
        to_check: list[tuple[Path, int | None, dict[str, Any]]] = [(Path(""), None, raw_variable)]
        while to_check:
            path, iteration, subdict = to_check.pop()
            for key, value in subdict.items():
                subpath = path / key
                if subpath in available_modules and isinstance(value, dict):
                    to_check.append((subpath, None, value))
                elif subpath in available_modules and isinstance(value, list):
                    if Flags.MODULE_REPEAT.is_enabled():
                        for no, module_variables in enumerate(value, 1):
                            if not isinstance(module_variables, dict):
                                raise ToolkitValueError(f"Variables under a module must be a dictionary: {subpath}.")
                            to_check.append((subpath, no, module_variables))
                    else:
                        raise ToolkitValueError(
                            f"Variables under a module cannot be a list: {subpath}. Please use a dictionary/mapping."
                        )
                elif isinstance(value, dict):
                    # Remove this check to support variables with dictionary values.
                    continue
                else:
                    hashable_values = tuple(value) if isinstance(value, list) else value
                    is_selected = selected_modules is None or path in selected_modules
                    variables.append(BuildVariable(key, hashable_values, is_selected, path, iteration))

        return cls(variables, source_path=source_path)

    @classmethod
    def load(cls, data: list[dict[str, Any]]) -> BuildVariables:
        """Loads the variables from a dictionary."""
        return cls([BuildVariable.load(variable) for variable in data])

    def get_module_variables(self, module: ModuleLocation) -> list[BuildVariables]:
        """Gets the variables for a specific module."""
        variables_by_key_by_iteration: dict[int | None, dict[str, list[BuildVariable]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for variable in self:
            if variable.location == module.relative_path or variable.location in module.parent_relative_paths:
                variables_by_key_by_iteration[variable.iteration][variable.key].append(variable)

        base_variables: dict[str, list[BuildVariable]] = variables_by_key_by_iteration.pop(None, {})
        variable_sets: list[dict[str, list[BuildVariable]]]
        if variables_by_key_by_iteration:
            # Combine each with the base variables
            variable_sets = []
            for _, variables_by_key in sorted(variables_by_key_by_iteration.items(), key=lambda x: x[0] or 0):
                for key, build_variable in base_variables.items():
                    variables_by_key[key].extend(build_variable)
                variable_sets.append(variables_by_key)
        else:
            variable_sets = [base_variables]

        return [
            BuildVariables(
                [
                    # We select the variable with the longest path to ensure that the most specific variable is selected
                    max(variables, key=lambda v: len(v.location.parts))
                    for variables in variable_set.values()
                ],
                source_path=self.source_path,
            )
            for variable_set in variable_sets
        ]

    @overload
    def replace(self, content: str, file_suffix: str = ".yaml", use_placeholder: Literal[False] = False) -> str: ...

    @overload
    def replace(
        self, content: str, file_suffix: str = ".yaml", use_placeholder: Literal[True] = True
    ) -> tuple[str, dict[str, BuildVariable]]: ...

    def replace(
        self, content: str, file_suffix: str = ".yaml", use_placeholder: bool = False
    ) -> str | tuple[str, dict[str, BuildVariable]]:
        variable_by_placeholder: dict[str, BuildVariable] = {}
        for variable in self:
            if not use_placeholder:
                replace = variable.value_variable
            else:
                replace = f"VARIABLE_{uuid.uuid4().hex[:8]}"
                variable_by_placeholder[replace] = variable

            _core_pattern = rf"{{{{\s*{variable.key}\s*}}}}"
            if file_suffix in {".yaml", ".yml", ".json"}:
                # Preserve data types
                pattern = _core_pattern
                if isinstance(replace, str) and (replace.isdigit() or replace.endswith(":")):
                    replace = f'"{replace}"'
                    pattern = rf"'{_core_pattern}'|{_core_pattern}|" + rf'"{_core_pattern}"'
                elif replace is None:
                    replace = "null"
                content = re.sub(pattern, str(replace), content)
            else:
                content = re.sub(_core_pattern, str(replace), content)
        if use_placeholder:
            return content, variable_by_placeholder
        else:
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
