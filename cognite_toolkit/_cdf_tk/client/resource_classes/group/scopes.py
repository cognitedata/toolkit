"""Scope definitions for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, Field, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import SCOPE_NAME
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class ScopeDefinition(BaseModelObject):
    """Base class for all scope definitions."""

    scope_name: str


class AllScope(ScopeDefinition):
    """Scope that applies to all resources."""

    scope_name: Literal["all"] = Field("all", exclude=True)


class CurrentUserScope(ScopeDefinition):
    """Scope that applies to the current user only."""

    scope_name: Literal["currentuserscope"] = Field("currentuserscope", exclude=True)


class DataSetScope(ScopeDefinition):
    """Scope limited to specific data sets by ID."""

    scope_name: Literal["datasetScope"] = Field("datasetScope", exclude=True)
    ids: list[int]


class IDScope(ScopeDefinition):
    """Scope limited to specific resource IDs."""

    scope_name: Literal["idScope"] = Field("idScope", exclude=True)
    ids: list[int]


class IDScopeLowerCase(ScopeDefinition):
    """Scope limited to specific resource IDs (lowercase variant)."""

    scope_name: Literal["idscope"] = Field("idscope", exclude=True)
    ids: list[int]


class SpaceIDScope(ScopeDefinition):
    """Scope limited to specific spaces by ID."""

    scope_name: Literal["spaceIdScope"] = Field("spaceIdScope", exclude=True)
    space_ids: list[str]


class AssetRootIDScope(ScopeDefinition):
    """Scope limited to assets under specific root assets."""

    scope_name: Literal["assetRootIdScope"] = Field("assetRootIdScope", exclude=True)
    root_ids: list[int]


class TableScope(ScopeDefinition):
    """Scope limited to specific RAW tables."""

    scope_name: Literal["tableScope"] = Field("tableScope", exclude=True)
    dbs_to_tables: dict[str, list[str]]


class ExtractionPipelineScope(ScopeDefinition):
    """Scope limited to specific extraction pipelines."""

    scope_name: Literal["extractionPipelineScope"] = Field("extractionPipelineScope", exclude=True)
    ids: list[int]


class InstancesScope(ScopeDefinition):
    """Scope limited to specific instances."""

    scope_name: Literal["instancesScope"] = Field("instancesScope", exclude=True)
    instances: list[str]


class PartitionScope(ScopeDefinition):
    """Scope limited to specific partitions."""

    scope_name: Literal["partition"] = Field("partition", exclude=True)
    partition_ids: list[int]


class ExperimentScope(ScopeDefinition):
    """Scope limited to specific experiments."""

    scope_name: Literal["experimentscope"] = Field("experimentscope", exclude=True)
    experiments: list[str]


class AppConfigScope(ScopeDefinition):
    """Scope limited to specific app configurations."""

    scope_name: Literal["appScope"] = Field("appScope", exclude=True)
    apps: list[str]


class PostgresGatewayUsersScope(ScopeDefinition):
    """Scope limited to specific PostgreSQL gateway users."""

    scope_name: Literal["usersScope"] = Field("usersScope", exclude=True)
    usernames: list[str]


class UnknownScope(ScopeDefinition):
    """Fallback class for unknown scope definitions."""

    scope_name: str


def _get_scope_name(cls: type[ScopeDefinition]) -> str | None:
    """Get the scope_name default value from a Pydantic model class."""
    field = cls.model_fields.get("scope_name")
    if field is not None and field.default is not None:
        return field.default
    return None


_KNOWN_SCOPES = {
    name: scope
    for scope in get_concrete_subclasses(ScopeDefinition)
    if (name := _get_scope_name(scope)) is not None and scope is not UnknownScope
}


def _handle_unknown_scope(value: Any) -> Any:
    if isinstance(value, dict) and isinstance(scope_name := value.get(SCOPE_NAME), str):
        scope_class = _KNOWN_SCOPES.get(scope_name)
        if scope_class:
            return TypeAdapter(scope_class).validate_python(value)
    return UnknownScope.model_validate(value)


Scope: TypeAlias = Annotated[
    (
        AllScope
        | CurrentUserScope
        | DataSetScope
        | IDScope
        | IDScopeLowerCase
        | SpaceIDScope
        | AssetRootIDScope
        | TableScope
        | ExtractionPipelineScope
        | InstancesScope
        | PartitionScope
        | ExperimentScope
        | AppConfigScope
        | PostgresGatewayUsersScope
        | UnknownScope
    ),
    BeforeValidator(_handle_unknown_scope),
]
