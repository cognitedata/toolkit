import builtins
import re
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Iterable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, Literal, Protocol, cast

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier, T_Identifier, T_RequestResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import (
    UNRESOLVED_VARIABLE_PATTERN,
    ValidatableResource,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    Insight,
    InternalValidatorException,
    T_Insight,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.file import yaml_find_unique_position


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


class RuleSetStatus(BaseModel):
    code: Literal["ready", "reduced", "skip", "unavailable"]
    message: str | None = None


class ResourceRef(Protocol):
    @property
    def identifier(self) -> Identifier: ...

    @property
    def type(self) -> ResourceType: ...

    @property
    def source(self) -> Path | Literal["cdf"]: ...

    @property
    def has_valid_syntax(self) -> bool: ...

    @property
    def dependencies(self) -> set[tuple[builtins.type[ResourceIO], Identifier]]: ...

    @property
    def variables(self) -> list[BuildVariable]: ...

    def as_request(
        self, crud: ResourceIO[T_Identifier, T_RequestResource, Any, Any], is_dry_run: bool = False
    ) -> ValidatableResource[T_RequestResource]:
        raise NotImplementedError()


class ResourceRefList(UserList[ResourceRef]):
    def get_resource_of_type(
        self, crud: ResourceIO[T_Identifier, T_RequestResource, Any, Any]
    ) -> Iterable[ValidatableResource[T_RequestResource]]:
        """Get the resources of the type handled by the given CRUD."""
        for _, validatable in self.get_refs_and_resource_of_type(crud):
            yield validatable

    def get_refs_and_resource_of_type(
        self, crud: ResourceIO[T_Identifier, T_RequestResource, Any, Any]
    ) -> Iterable[tuple[ResourceRef, ValidatableResource[T_RequestResource]]]:
        """Get the resources of the type handled by the given CRUD, together with their reference."""
        resource_type = crud.as_resource_type()
        for resource in self.data:
            if resource.type != resource_type or not resource.has_valid_syntax:
                continue
            yield resource, resource.as_request(crud, is_dry_run=True)


class ToolkitGlobalRuleSet(ABC):
    """Validation of all modules as a whole.

    This can output different

    Args:
        resources: The resources to validate.
        client: The ToolkitClient to use. This is required by some rules.
    """

    CODE_PREFIX: ClassVar[str]
    DISPLAY_NAME: ClassVar[str]

    def __init__(
        self,
        resources: Sequence[ResourceRef],
        results: Sequence[DeploymentResult] | None = None,
        client: ToolkitClient | None = None,
    ) -> None:
        self.resources = ResourceRefList(resources)
        self.results = results
        self.client = client

    def _load_client(self) -> ToolkitClient:
        """Client used to instantiate CRUDs for loading local resources. Loading in dry-run does not call CDF."""
        if self.client is not None:
            return self.client
        return cast(ToolkitClient, SimpleNamespace(console=None))

    @abstractmethod
    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    @abstractmethod
    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        raise NotImplementedError()


_GLOBAL_RULES_REGISTRY: list[type[ToolkitGlobalRuleSet]] | None = None


def get_global_rules_registry(force_reload: bool = False) -> list[type[ToolkitGlobalRuleSet]]:
    """Get the registry of rules, optionally forcing a reload."""
    global _GLOBAL_RULES_REGISTRY
    if _GLOBAL_RULES_REGISTRY is None or force_reload:
        _GLOBAL_RULES_REGISTRY = list(get_concrete_subclasses(ToolkitGlobalRuleSet))  # type: ignore[type-abstract]
    return _GLOBAL_RULES_REGISTRY
