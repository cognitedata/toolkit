from __future__ import annotations

import re
from collections import UserDict, UserList
from dataclasses import dataclass

__all__ = ["Variable", "Variables", "ModuleMeta", "ModuleMetaList"]

from pathlib import Path
from typing import Any, cast

import pandas as pd

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import ConfigEntry, Environment, InitConfigYAML
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME

NOT_SET = object()

_DUMMY_ENVIRONMENT = Environment(
    name="not used",
    project="not used",
    build_type="dev",
    selected=[],
)


@dataclass
class Variable:
    name: str
    default: str
    description: str | None = None
    _value: str | object = NOT_SET

    @property
    def value(self) -> str:
        if isinstance(self._value, str):
            return self._value
        return self.default

    @value.setter
    def value(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Variable value must be a string")
        self._value = value

    @classmethod
    def _load(cls, entry: ConfigEntry) -> Variable:
        description: str | None = None
        if entry.default_comment:
            description = entry.default_comment.comment
        _value: str | object = NOT_SET
        if not (isinstance(entry.default_value, str) and re.match(r"<.*?>", entry.default_value)):
            _value = entry.default_value

        return cls(
            name=entry.key_path[-1],
            _value=_value,
            default=cast(str, entry.default_value),
            description=description,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(value={self.value})"


class Variables(UserDict):
    def __init__(self, collection: dict[str, Variable] | None = None) -> None:
        super().__init__(collection or {})

    @classmethod
    def _load(cls, module_path: Path, default_variables: dict[tuple[str, ...], ConfigEntry]) -> Variables:
        loaded_variables = InitConfigYAML(_DUMMY_ENVIRONMENT).load_variables(module_path)
        variables: dict[str, Variable] = {}
        module_key_path = tuple(module_path.relative_to(COGNITE_MODULES_PATH).parts)
        for (*_, variable_name), value in loaded_variables.items():
            variable_module_key = [*module_key_path, "first_pop"]
            while variable_module_key:
                variable_module_key.pop()
                key_path = (InitConfigYAML._variables, *variable_module_key, variable_name)
                if default := default_variables.get(key_path):
                    value.default_value = default.default_value
                    value.default_comment = default.default_comment
                    variables[variable_name] = Variable._load(value)
                    break

            else:
                raise ValueError(f"Variable {variable_name} missing from default config in {module_path!r}")

        return cls(variables)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self)})"

    def _repr_html_(self) -> str:
        return pd.DataFrame(
            [
                {
                    "name": variable.name,
                    "value": variable.value,
                    "description": variable.description,
                }
                for variable in self.values()
            ]
        )._repr_html_()


@dataclass(frozen=True)
class ModuleMeta:
    name: str
    variables: Variables
    resource_types: tuple[str, ...]
    packages: frozenset[str]
    _source: Path
    _readme: str | None = None

    @classmethod
    def _load(
        cls, module_path: Path, packages: frozenset[str], default_variables: dict[tuple[str, ...], ConfigEntry]
    ) -> ModuleMeta:
        readme: str | None = None
        if (readme_path := module_path / "README.md").exists():
            readme = readme_path.read_text(encoding="utf-8")
        resource_types = tuple(
            resource_path.name for resource_path in module_path.iterdir() if resource_path.name in LOADER_BY_FOLDER_NAME
        )

        return cls(
            name=module_path.name,
            variables=Variables._load(module_path, default_variables),
            resource_types=resource_types,
            packages=packages,
            _source=module_path,
            _readme=readme,
        )

    def _repr_html_(self) -> str:
        return (
            pd.Series(
                {
                    "name": self.name,
                    "variables": len(self.variables),
                    "resource_types": self.resource_types,
                }
            )
            .to_frame("Value")
            ._repr_html_()
        )

    @property
    def info(self) -> None:
        try:
            from IPython.display import Markdown, display
        except ImportError:
            print(str(self))
            return
        display(Markdown(self._readme or ""))


class ModuleMetaList(UserList):
    @property
    def names(self) -> list[str]:
        return [module.name for module in self.data]

    def __getitem__(self, item: str | int | slice) -> ModuleMeta:  # type: ignore[override]
        if isinstance(item, (int, slice)):
            return cast(ModuleMeta, super().__getitem__(item))
        for module in self.data:
            if module.name == item:
                return module
        raise KeyError(f"Module {item!r} not found")

    def __setitem__(self, key: Any, value: Any) -> None:
        if not isinstance(value, ModuleMeta):
            raise TypeError(f"Expected ModuleMeta, got {type(value).__name__}")
        super().__setitem__(key, value)

    def _repr_html_(self) -> str:
        return pd.DataFrame(
            [
                {
                    "name": module.name,
                    "variables": len(module.variables),
                    "resource_types": module.resource_types,
                }
                for module in self.data
            ]
        )._repr_html_()
