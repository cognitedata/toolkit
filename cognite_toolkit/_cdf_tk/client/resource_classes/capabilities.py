from typing import TypeVar

from cognite.client.data_classes.capabilities import (
    AllScope,
    AppConfigScope,
    AssetRootIDScope,
    Capability,
    CurrentUserScope,
    DataSetScope,
    ExperimentsScope,
    ExtractionPipelineScope,
    IDScope,
    IDScopeLowerCase,
    InstancesScope,
    LegacyDataModelScope,
    LegacySpaceScope,
    PartitionScope,
    PostgresGatewayUsersScope,
    SpaceIDScope,
    TableScope,
)

T_Scope = TypeVar("T_Scope", bound=Capability.Scope)


def scope_intersection(scope1: T_Scope, scope2: T_Scope | None) -> Capability.Scope | None:
    """Returns the intersection of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns None (intersection with nothing is nothing).
    If both scopes are the same, returns that scope.
    If scopes are different types, raise an ValueError.
    """
    if scope2 is None:
        return None
    if type(scope1) is not type(scope2):
        raise ValueError("Cannot intersect scopes of different types")
    if isinstance(scope1, AllScope) and isinstance(scope2, AllScope):
        return AllScope()
    elif isinstance(scope1, AppConfigScope) and isinstance(scope2, AppConfigScope):
        if apps := sorted(set(scope1.apps) & set(scope2.apps)):
            return AppConfigScope(apps=apps)
        return None
    elif isinstance(scope1, DataSetScope) and isinstance(scope2, DataSetScope):
        if ids := sorted(set(scope1.ids) & set(scope2.ids)):
            return DataSetScope(ids=ids)
        return None
    elif isinstance(scope1, IDScope) and isinstance(scope2, IDScope):
        if ids := sorted(set(scope1.ids) & set(scope2.ids)):
            return IDScope(ids=ids)
        return None
    elif isinstance(scope1, IDScopeLowerCase) and isinstance(scope2, IDScopeLowerCase):
        if ids := sorted(set(scope1.ids) & set(scope2.ids)):
            return IDScopeLowerCase(ids=ids)
        return None
    elif isinstance(scope1, SpaceIDScope) and isinstance(scope2, SpaceIDScope):
        if intersection_space_ids := sorted(set(scope1.space_ids) & set(scope2.space_ids)):
            return SpaceIDScope(space_ids=intersection_space_ids)
        return None
    elif isinstance(scope1, AssetRootIDScope) and isinstance(scope2, AssetRootIDScope):
        if intersection_root_ids := sorted(set(scope1.root_ids) & set(scope2.root_ids)):
            return AssetRootIDScope(root_ids=intersection_root_ids)
        return None
    elif isinstance(scope1, CurrentUserScope) and isinstance(scope2, CurrentUserScope):
        return CurrentUserScope()
    elif isinstance(scope1, ExtractionPipelineScope) and isinstance(scope2, ExtractionPipelineScope):
        if intersection_ids := sorted(set(scope1.ids) & set(scope2.ids)):
            return ExtractionPipelineScope(ids=intersection_ids)
        return None
    elif isinstance(scope1, TableScope) and isinstance(scope2, TableScope):
        intersection_tables = set(scope1.dbs_to_tables.keys()) & set(scope2.dbs_to_tables.keys())
        if intersection_tables:
            return TableScope(
                dbs_to_tables={
                    db: sorted(set(scope1.dbs_to_tables[db]) & set(scope2.dbs_to_tables[db]))
                    for db in sorted(intersection_tables)
                }
            )
        return None
    elif isinstance(scope1, InstancesScope) and isinstance(scope2, InstancesScope):
        if intersection_instances := sorted(set(scope1.instances) & set(scope2.instances)):
            return InstancesScope(instances=intersection_instances)
        return None
    elif isinstance(scope1, PartitionScope) and isinstance(scope2, PartitionScope):
        if intersection_partition_ids := sorted(set(scope1.partition_ids) & set(scope2.partition_ids)):
            return PartitionScope(partition_ids=intersection_partition_ids)
        return None
    elif isinstance(scope1, LegacySpaceScope) and isinstance(scope2, LegacySpaceScope):
        if intersection_external_ids := sorted(set(scope1.external_ids) & set(scope2.external_ids)):
            return LegacySpaceScope(external_ids=intersection_external_ids)
        return None
    elif isinstance(scope1, LegacyDataModelScope) and isinstance(scope2, LegacyDataModelScope):
        if intersection_external_ids := sorted(set(scope1.external_ids) & set(scope2.external_ids)):
            return LegacyDataModelScope(external_ids=intersection_external_ids)
        return None
    elif isinstance(scope1, ExperimentsScope) and isinstance(scope2, ExperimentsScope):
        if intersection_experiments := sorted(set(scope1.experiments) & set(scope2.experiments)):
            return ExperimentsScope(experiments=intersection_experiments)
        return None
    elif isinstance(scope1, PostgresGatewayUsersScope) and isinstance(scope2, PostgresGatewayUsersScope):
        if intersection_usernames := sorted(set(scope1.usernames) & set(scope2.usernames)):
            return PostgresGatewayUsersScope(usernames=intersection_usernames)
        return None
    else:
        raise TypeError(f"Unknown scope type {type(scope1)!r}")


def scope_union(scope1: T_Scope, scope2: T_Scope | None) -> Capability.Scope:
    """
    Returns the union of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns the other scope.
    If both scopes are the same, returns the union of the two scopes.
    If scopes are different types, raise an ValueError.
    """
    if scope2 is None:
        return scope1
    if type(scope1) is not type(scope2):
        raise ValueError("Cannot union scopes of different types")
    if isinstance(scope1, AllScope) or isinstance(scope2, AllScope):
        return AllScope()
    elif isinstance(scope1, AppConfigScope) and isinstance(scope2, AppConfigScope):
        return AppConfigScope(apps=sorted(set(scope1.apps) | set(scope2.apps)))
    elif isinstance(scope1, DataSetScope) and isinstance(scope2, DataSetScope):
        return DataSetScope(ids=sorted(set(scope1.ids) | set(scope2.ids)))
    elif isinstance(scope1, IDScope) and isinstance(scope2, IDScope):
        return IDScope(ids=sorted(set(scope1.ids) | set(scope2.ids)))
    elif isinstance(scope1, IDScopeLowerCase) and isinstance(scope2, IDScopeLowerCase):
        return IDScopeLowerCase(ids=sorted(set(scope1.ids) | set(scope2.ids)))
    elif isinstance(scope1, SpaceIDScope) and isinstance(scope2, SpaceIDScope):
        return SpaceIDScope(space_ids=sorted(set(scope1.space_ids) | set(scope2.space_ids)))
    elif isinstance(scope1, AssetRootIDScope) and isinstance(scope2, AssetRootIDScope):
        return AssetRootIDScope(root_ids=sorted(set(scope1.root_ids) | set(scope2.root_ids)))
    elif isinstance(scope1, CurrentUserScope) and isinstance(scope2, CurrentUserScope):
        return CurrentUserScope()
    elif isinstance(scope1, ExtractionPipelineScope) and isinstance(scope2, ExtractionPipelineScope):
        return ExtractionPipelineScope(ids=sorted(set(scope1.ids) | set(scope2.ids)))
    elif isinstance(scope1, TableScope) and isinstance(scope2, TableScope):
        union_tables = set(scope1.dbs_to_tables.keys()) | set(scope2.dbs_to_tables.keys())
        return TableScope(
            dbs_to_tables={
                db: sorted(set(scope1.dbs_to_tables.get(db, [])) | set(scope2.dbs_to_tables.get(db, [])))
                for db in sorted(union_tables)
            }
        )
    elif isinstance(scope1, InstancesScope) and isinstance(scope2, InstancesScope):
        return InstancesScope(
            instances=sorted(set(scope1.instances) | set(scope2.instances)),
        )
    elif isinstance(scope1, PartitionScope) and isinstance(scope2, PartitionScope):
        return PartitionScope(
            partition_ids=sorted(set(scope1.partition_ids) | set(scope2.partition_ids)),
        )
    elif isinstance(scope1, LegacySpaceScope) and isinstance(scope2, LegacySpaceScope):
        return LegacySpaceScope(
            external_ids=sorted(set(scope1.external_ids) | set(scope2.external_ids)),
        )
    elif isinstance(scope1, LegacyDataModelScope) and isinstance(scope2, LegacyDataModelScope):
        return LegacyDataModelScope(
            external_ids=sorted(set(scope1.external_ids) | set(scope2.external_ids)),
        )
    elif isinstance(scope1, ExperimentsScope) and isinstance(scope2, ExperimentsScope):
        return ExperimentsScope(
            experiments=sorted(set(scope1.experiments) | set(scope2.experiments)),
        )
    elif isinstance(scope1, PostgresGatewayUsersScope) and isinstance(scope2, PostgresGatewayUsersScope):
        return PostgresGatewayUsersScope(
            usernames=sorted(set(scope1.usernames) | set(scope2.usernames)),
        )
    else:
        raise TypeError(f"Unknown scope type {type(scope1)!r}")
