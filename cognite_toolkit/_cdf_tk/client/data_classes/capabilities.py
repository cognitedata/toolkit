from typing import TypeVar, cast

from cognite.client.data_classes.capabilities import Capability
from cognite_toolkit._cdf_tk.resource_classes.capabilities import (
    AllScope,
    AppConfigScope,
    AssetRootIDScope,
    CurrentUserScope,
    DataSetScope,
    ExperimentScope,
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


def scope_intersection(scope1: T_Scope | None, scope2: T_Scope | None) -> T_Scope | None:
    """
    Returns the intersection of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns None (intersection with nothing is nothing).
    If both scopes are the same, returns that scope.
    If scopes are different types, handles the intersection based on scope hierarchy.
    """
    if scope1 is None or scope2 is None:
        return None
    if scope1 == scope2:
        return scope1
    
    # If one is AllScope, intersection is the other scope
    if isinstance(scope1, AllScope):
        return scope2
    if isinstance(scope2, AllScope):
        return scope1
    
    # If both are the same type, handle intersection based on scope attributes
    if type(scope1) == type(scope2):
        # Handle scopes with ids attribute
        if isinstance(scope1, DataSetScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            intersected_ids = sorted(list(ids1 & ids2))
            return cast(T_Scope, DataSetScope(ids=intersected_ids))
        
        if isinstance(scope1, IDScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, IDScope(ids=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, IDScopeLowerCase):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, IDScopeLowerCase(ids=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, ExtractionPipelineScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, ExtractionPipelineScope(ids=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, SpaceIDScope):
            ids1 = set(scope1.space_ids)
            ids2 = set(scope2.space_ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, SpaceIDScope(spaceIds=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, AssetRootIDScope):
            ids1 = set(scope1.root_ids)
            ids2 = set(scope2.root_ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, AssetRootIDScope(rootIds=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, InstancesScope):
            ids1 = set(scope1.instances)
            ids2 = set(scope2.instances)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, InstancesScope(instances=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, LegacyDataModelScope):
            ids1 = set(scope1.external_ids)
            ids2 = set(scope2.external_ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, LegacyDataModelScope(externalIds=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, LegacySpaceScope):
            ids1 = set(scope1.external_ids)
            ids2 = set(scope2.external_ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, LegacySpaceScope(externalIds=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, PartitionScope):
            ids1 = set(scope1.partition_ids)
            ids2 = set(scope2.partition_ids)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, PartitionScope(partitionIds=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, PostgresGatewayUsersScope):
            ids1 = set(scope1.usernames)
            ids2 = set(scope2.usernames)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, PostgresGatewayUsersScope(usernames=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, ExperimentScope):
            ids1 = set(scope1.experiments)
            ids2 = set(scope2.experiments)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, ExperimentScope(experiments=intersected_ids))
            else:
                return None
        
        if isinstance(scope1, AppConfigScope):
            ids1 = set(scope1.apps)
            ids2 = set(scope2.apps)
            intersected_ids = list(ids1 & ids2)
            if intersected_ids:
                return cast(T_Scope, AppConfigScope(apps=intersected_ids))
            else:
                return None
        
        # Handle scopes without collection attributes
        if isinstance(scope1, (AllScope, CurrentUserScope)):
            return scope1
        
        # Handle TableScope - complex case, return None for now
        if isinstance(scope1, TableScope):
            return None
    
    # For different incompatible scope types, intersection is None
    return None


def scope_union(scope1: T_Scope | None, scope2: T_Scope | None) -> T_Scope | None:
    """
    Returns the union of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns the other scope.
    If both scopes are the same, returns that scope.
    If scopes are different types, handles the union based on scope hierarchy.
    """
    if scope1 is None:
        return scope2
    if scope2 is None:
        return scope1
    if scope1 == scope2:
        return scope1
    
    # If either is AllScope, union is AllScope
    if isinstance(scope1, AllScope) or isinstance(scope2, AllScope):
        return cast(T_Scope, AllScope())
    
    # If both are the same type, handle union based on scope attributes
    if type(scope1) == type(scope2):
        # Handle scopes with ids attribute
        if isinstance(scope1, DataSetScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, DataSetScope(ids=union_ids))
        
        if isinstance(scope1, IDScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, IDScope(ids=union_ids))
        
        if isinstance(scope1, IDScopeLowerCase):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, IDScopeLowerCase(ids=union_ids))
        
        if isinstance(scope1, ExtractionPipelineScope):
            ids1 = set(scope1.ids)
            ids2 = set(scope2.ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, ExtractionPipelineScope(ids=union_ids))
        
        if isinstance(scope1, SpaceIDScope):
            ids1 = set(scope1.space_ids)
            ids2 = set(scope2.space_ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, SpaceIDScope(spaceIds=union_ids))
        
        if isinstance(scope1, AssetRootIDScope):
            ids1 = set(scope1.root_ids)
            ids2 = set(scope2.root_ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, AssetRootIDScope(rootIds=union_ids))
        
        if isinstance(scope1, InstancesScope):
            ids1 = set(scope1.instances)
            ids2 = set(scope2.instances)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, InstancesScope(instances=union_ids))
        
        if isinstance(scope1, LegacyDataModelScope):
            ids1 = set(scope1.external_ids)
            ids2 = set(scope2.external_ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, LegacyDataModelScope(externalIds=union_ids))
        
        if isinstance(scope1, LegacySpaceScope):
            ids1 = set(scope1.external_ids)
            ids2 = set(scope2.external_ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, LegacySpaceScope(externalIds=union_ids))
        
        if isinstance(scope1, PartitionScope):
            ids1 = set(scope1.partition_ids)
            ids2 = set(scope2.partition_ids)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, PartitionScope(partitionIds=union_ids))
        
        if isinstance(scope1, PostgresGatewayUsersScope):
            ids1 = set(scope1.usernames)
            ids2 = set(scope2.usernames)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, PostgresGatewayUsersScope(usernames=union_ids))
        
        if isinstance(scope1, ExperimentScope):
            ids1 = set(scope1.experiments)
            ids2 = set(scope2.experiments)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, ExperimentScope(experiments=union_ids))
        
        if isinstance(scope1, AppConfigScope):
            ids1 = set(scope1.apps)
            ids2 = set(scope2.apps)
            union_ids = list(ids1 | ids2)
            return cast(T_Scope, AppConfigScope(apps=union_ids))
        
        # Handle scopes without collection attributes
        if isinstance(scope1, (AllScope, CurrentUserScope)):
            return scope1
        
        # Handle TableScope - complex case, return None for now
        if isinstance(scope1, TableScope):
            return None
    
    # For different incompatible scope types, return None
    return None
