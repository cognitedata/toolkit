"""Scope definitions for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from types import MappingProxyType
from typing import Any, ClassVar

from pydantic import model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject


class Scope(BaseModelObject):
    """Base class for all scope definitions."""

    _scope_name: ClassVar[str]

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_scope_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._scope_name is None:
            raise ValueError("Scope name is not set")
        serialized_data = handler(self)
        return {self._scope_name: serialized_data}


class AllScope(Scope):
    """Scope that applies to all resources."""

    _scope_name: ClassVar[str] = "all"


class CurrentUserScope(Scope):
    """Scope that applies to the current user only."""

    _scope_name: ClassVar[str] = "currentuserscope"


class DataSetScope(Scope):
    """Scope limited to specific data sets by ID."""

    _scope_name: ClassVar[str] = "datasetScope"
    ids: list[int]


class IDScope(Scope):
    """Scope limited to specific resource IDs."""

    _scope_name: ClassVar[str] = "idScope"
    ids: list[int]


class IDScopeLowerCase(Scope):
    """Scope limited to specific resource IDs (lowercase variant)."""

    _scope_name: ClassVar[str] = "idscope"
    ids: list[int]


class SpaceIDScope(Scope):
    """Scope limited to specific spaces by ID."""

    _scope_name: ClassVar[str] = "spaceIdScope"
    space_ids: list[str]


class AssetRootIDScope(Scope):
    """Scope limited to assets under specific root assets."""

    _scope_name: ClassVar[str] = "assetRootIdScope"
    root_ids: list[int]


class TableScope(Scope):
    """Scope limited to specific RAW tables."""

    _scope_name: ClassVar[str] = "tableScope"
    dbs_to_tables: dict[str, list[str]]


class ExtractionPipelineScope(Scope):
    """Scope limited to specific extraction pipelines."""

    _scope_name: ClassVar[str] = "extractionPipelineScope"
    ids: list[int]


class InstancesScope(Scope):
    """Scope limited to specific instances."""

    _scope_name: ClassVar[str] = "instancesScope"
    instances: list[str]


class PartitionScope(Scope):
    """Scope limited to specific partitions."""

    _scope_name: ClassVar[str] = "partition"
    partition_ids: list[int]


class ExperimentScope(Scope):
    """Scope limited to specific experiments."""

    _scope_name: ClassVar[str] = "experimentscope"
    experiments: list[str]


class AppConfigScope(Scope):
    """Scope limited to specific app configurations."""

    _scope_name: ClassVar[str] = "appScope"
    apps: list[str]


class PostgresGatewayUsersScope(Scope):
    """Scope limited to specific PostgreSQL gateway users."""

    _scope_name: ClassVar[str] = "usersScope"
    usernames: list[str]


# Build scope lookup
_SCOPE_CLASS_BY_NAME: MappingProxyType[str, type[Scope]] = MappingProxyType(
    {cls._scope_name: cls for cls in Scope.__subclasses__()}
)


def parse_scope(data: dict[str, Any]) -> Scope:
    """Parse a scope from a dictionary."""
    if not isinstance(data, dict) or len(data) != 1:
        raise ValueError(f"Invalid scope format: {data}")

    scope_name, scope_content = next(iter(data.items()))
    if scope_name not in _SCOPE_CLASS_BY_NAME:
        # Return an AllScope as fallback for unknown scopes
        return AllScope()

    scope_cls = _SCOPE_CLASS_BY_NAME[scope_name]
    if scope_content:
        return scope_cls.model_validate(scope_content)
    return scope_cls()
