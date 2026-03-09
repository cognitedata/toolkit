"""Set operations (intersection and union) for scope definitions."""

from typing import Any

from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import (
    AllScope,
    ScopeDefinition,
    TableScope,
    UnknownScope,
)


def _data_fields(scope: ScopeDefinition) -> dict[str, Any]:
    """Return field name-value pairs excluding scope_name."""
    return {name: getattr(scope, name) for name in type(scope).model_fields if name != "scope_name"}


def scope_intersection(*scopes: ScopeDefinition) -> ScopeDefinition | None:
    """Return the intersection of all given scopes, or None if the result is empty.

    Rules:
      - If no scopes are given, returns None.
      - AllScope is the identity element: intersection with AllScope yields the other scope.
        If all scopes are AllScope, returns AllScope.
      - Non-AllScope scopes must all be the same concrete type.
      - Scopes with no data fields (CurrentUserScope) are returned as-is.
      - List fields are set-intersected; an empty result means None.
      - TableScope intersects the db keys and per-db table lists.
    """
    if not scopes:
        return None

    non_all = [s for s in scopes if not isinstance(s, AllScope)]
    if not non_all:
        return AllScope()
    scopes = tuple(non_all)

    first = scopes[0]
    if any(type(s) is not type(first) for s in scopes[1:]):
        raise ValueError("Cannot intersect scopes of different types")
    if isinstance(first, UnknownScope):
        raise TypeError("Cannot intersect unknown scopes")

    fields = _data_fields(first)

    if not fields:
        return type(first)()

    if isinstance(first, TableScope):
        table_scopes = [s for s in scopes if isinstance(s, TableScope)]
        common_dbs = sorted(set.intersection(*(set(s.dbs_to_tables) for s in table_scopes)))
        if not common_dbs:
            return None
        return TableScope(
            dbs_to_tables={
                db: sorted(set.intersection(*(set(s.dbs_to_tables[db]) for s in table_scopes))) for db in common_dbs
            }
        )

    merged: dict[str, Any] = {}
    for name in fields:
        values = [set(getattr(s, name)) for s in scopes]
        result = sorted(set.intersection(*values))
        if not result:
            return None
        merged[name] = result

    return type(first)(**merged)


def scope_union(*scopes: ScopeDefinition) -> ScopeDefinition:
    """Return the union of all given scopes.

    Rules:
      - At least one scope must be provided.
      - AllScope is the absorbing element: union with AllScope always yields AllScope.
      - Non-AllScope scopes must all be the same concrete type.
      - Scopes with no data fields (CurrentUserScope) are returned as-is.
      - List fields are set-unioned.
      - TableScope unions the db keys and per-db table lists.
    """
    if not scopes:
        raise ValueError("At least one scope is required for union")

    if any(isinstance(s, AllScope) for s in scopes):
        return AllScope()

    first = scopes[0]
    if any(type(s) is not type(first) for s in scopes[1:]):
        raise ValueError("Cannot union scopes of different types")
    if isinstance(first, UnknownScope):
        raise TypeError("Cannot union unknown scopes")

    fields = _data_fields(first)

    if not fields:
        return type(first)()

    if isinstance(first, TableScope):
        table_scopes = [s for s in scopes if isinstance(s, TableScope)]
        all_dbs = sorted(set.union(*(set(s.dbs_to_tables) for s in table_scopes)))
        return TableScope(
            dbs_to_tables={
                db: sorted(set.union(*(set(s.dbs_to_tables.get(db, [])) for s in table_scopes))) for db in all_dbs
            }
        )

    merged: dict[str, Any] = {}
    for name in fields:
        values = [set(getattr(s, name)) for s in scopes]
        merged[name] = sorted(set.union(*values))

    return type(first)(**merged)


def scope_difference(scope1: ScopeDefinition, scope2: ScopeDefinition | None) -> ScopeDefinition | None:
    """Return the difference of two scopes (scope1 - scope2), or None if the result is empty.

    Rules:
      - If scope2 is None, returns scope1.
      - AllScope minus any scope yields AllScope; any scope minus AllScope yields None.
      - Non-AllScope scopes must be the same concrete type.
      - Scopes with no data fields (CurrentUserScope) always yield None (A - A = empty).
      - List fields are set-differenced; an empty result means None.
      - TableScope differences per-db table lists and drops empty dbs.
    """
    if scope2 is None:
        return scope1

    if isinstance(scope1, AllScope):
        return None if isinstance(scope2, AllScope) else AllScope()

    if isinstance(scope2, AllScope):
        return None

    if type(scope1) is not type(scope2):
        raise ValueError("Cannot difference scopes of different types")
    if isinstance(scope1, UnknownScope):
        raise TypeError("Cannot difference unknown scopes")

    fields = _data_fields(scope1)

    if not fields:
        return None

    if isinstance(scope1, TableScope) and isinstance(scope2, TableScope):
        result_dbs: dict[str, list[str]] = {}
        for db, tables in scope1.dbs_to_tables.items():
            if db in scope2.dbs_to_tables:
                remaining = sorted(set(tables) - set(scope2.dbs_to_tables[db]))
                if remaining:
                    result_dbs[db] = remaining
            else:
                result_dbs[db] = sorted(tables)
        if not result_dbs:
            return None
        return TableScope(dbs_to_tables=result_dbs)

    merged: dict[str, Any] = {}
    for name in fields:
        result = sorted(set(getattr(scope1, name)) - set(getattr(scope2, name)))
        if not result:
            return None
        merged[name] = result

    return type(scope1)(**merged)
