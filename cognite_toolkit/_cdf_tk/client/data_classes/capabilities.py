from typing import TypeVar

from cognite.client.data_classes.capabilities import Capability

T_Scope = TypeVar("T_Scope", bound=Capability.Scope)


def scope_intersection(scope1: T_Scope | None, scope2: T_Scope | None) -> T_Scope | None:
    """
    Returns the intersection of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns the other scope.
    If both scopes are the same, returns that scope.
    If scopes are different, raises an error.
    """
    if scope1 is None:
        return scope2
    if scope2 is None:
        return scope1
    if scope1 == scope2:
        return scope1
    raise NotImplementedError()


def scope_union(scope1: T_Scope | None, scope2: T_Scope | None) -> T_Scope | None:
    """
    Returns the union of two scopes.
    If both scopes are None, returns None.
    If one scope is None, returns the other scope.
    If both scopes are the same, returns that scope.
    If scopes are different, raises an error.
    """
    if scope1 is None:
        return scope2
    if scope2 is None:
        return scope1
    if scope1 == scope2:
        return scope1
    raise NotImplementedError()
