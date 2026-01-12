"""Scope definitions for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject


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


Scope = Annotated[
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
    Field(discriminator="scope_name"),
]
