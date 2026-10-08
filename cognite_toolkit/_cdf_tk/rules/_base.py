import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from typing import ClassVar, Literal

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import UNRESOLVED_VARIABLE_PATTERN
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    Insight,
    InternalValidatorException,
    T_Insight,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable, Module, SuccessfulReadYAMLFile
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.file import yaml_find_unique_position
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource


def quote_identifier(identifier: Identifier) -> str:
    """Formats an identifier for messages, e.g. 'my_id' instead of "externalId='my_id'"."""
    values = list(identifier.dump().values())
    if len(values) == 1 and isinstance(values[0], str):
        return f"'{values[0]}'"
    text = str(identifier)
    return text if "'" in text else f"'{text}'"


def _same_length_placeholder(match: re.Match[str]) -> str:
    return "x" * len(match.group())


def with_position(
    insight: T_Insight,
    *,
    values: Sequence[str] = (),
    keys: Sequence[str] = (),
    variables: Sequence[BuildVariable] = (),
) -> T_Insight:
    """Sets the line and column of an insight to where it is found in its source file.

    The first of the keys (mapping keys), then the first of the values (mapping values or list items), that
    occurs exactly once in the file is used. The insight is left without a position if nothing is found exactly
    once.

    Args:
        insight: The insight to set the position of.
        values: The values to look for, for example, the identifier of a referenced resource.
        keys: The keys to look for, for example, the field with an invalid value.
        variables: The variables used when building the source file. They are substituted before searching, such
            that values built from variables, e.g., 'sp_{{ location }}_assets', are found. Substituted values
            can shift the column of text after them on the same line, but not where a key or value starts.
    """
    if not Flags.V09.is_enabled():
        return insight
    try:
        content = insight.source_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return insight
    content = BuildVariable.substitute(content, list(variables), insight.source_file.suffix)
    # Variables such as {{ space }} can make the source file invalid YAML. They are replaced by text of the same
    # length, such that the positions in the file are kept.
    content = UNRESOLVED_VARIABLE_PATTERN.sub(_same_length_placeholder, content)
    candidates = [(key, True) for key in keys] + [(value, False) for value in values]
    for text, as_key in candidates:
        if position := yaml_find_unique_position(content, text, as_key=as_key):
            insight.line, insight.column = position.line, position.column
            break
    return insight


def identifier_values(identifier: Identifier) -> list[str]:
    """The string values of an identifier, to search for in the YAML referencing it. The external ID comes first."""
    dumped = identifier.dump()
    ordered = [dumped[key] for key in dumped if key == "externalId"] + [
        value for key, value in dumped.items() if key != "externalId"
    ]
    return [value for value in ordered if isinstance(value, str)]


EXECUTE_RULE_STATUS: tuple[Literal["ready", "reduced", "skip", "unavailable"], ...] = (
    "ready",
    "reduced",
)


class ToolkitLocalRule(ABC):
    """Rule validating a module

    Args:
        module: The module to validate.
    """

    CODE: ClassVar[str]
    LEGACY_CODE: ClassVar[str]  # Used when the v09 flag is not enabled
    IS_ALPHA: ClassVar[bool] = False
    IS_FIXABLE: ClassVar[bool] = False

    def __init__(self, module: Module) -> None:
        self.module = module

    @abstractmethod
    def validate(self) -> Iterable[Insight]:
        raise NotImplementedError()

    def _get_validated_resources_with_file(self) -> Iterable[tuple[ToolkitResource, SuccessfulReadYAMLFile]]:
        for file in self.module.files:
            if not isinstance(file, SuccessfulReadYAMLFile):
                continue
            for resource in file.resources:
                if resource.validated is not None:
                    yield resource.validated, file


class RuleSetStatus(BaseModel):
    code: Literal["ready", "reduced", "skip", "unavailable"]
    message: str | None = None


class ToolkitGlobalRuleSet(ABC):
    """Validation of all modules as a whole.

    This can output different

    Args:
        module: The module to validate.
        client: The ToolkitClient to use. This is required by some rules.
    """

    CODE_PREFIX: ClassVar[str]
    DISPLAY_NAME: ClassVar[str]
    def __init__(self, modules: list[BuiltModule], client: ToolkitClient | None = None) -> None:
        self.modules = modules
        self.client = client

    @abstractmethod
    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    @abstractmethod
    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        raise NotImplementedError()
