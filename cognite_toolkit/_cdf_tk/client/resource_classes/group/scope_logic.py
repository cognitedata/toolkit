"""Set operations (intersection and union) for scope definitions."""

from typing import Any

from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import (
    ScopeDefinition,
    TableScope,
    UnknownScope,
)


def _data_fields(scope: ScopeDefinition) -> dict[str, Any]:
    """Return field name-value pairs excluding scope_name."""
    return {name: getattr(scope, name) for name in scope.model_fields if name != "scope_name"}


def scope_intersection(scope1: ScopeDefinition, scope2: ScopeDefinition | None) -> ScopeDefinition | None:
    """Return the intersection of two scopes, or None if the result is empty.

    Rules:
      - If scope2 is None, returns None.
      - Both scopes must be the same concrete type.
      - Scopes with no data fields (AllScope, CurrentUserScope) are returned as-is.
      - List fields are set-intersected; an empty result means None.
      - TableScope intersects the db keys and per-db table lists.
    """
    if scope2 is None:
        return None
    if type(scope1) is not type(scope2):
        raise ValueError("Cannot intersect scopes of different types")
    if isinstance(scope1, UnknownScope):
        raise TypeError("Cannot intersect unknown scopes")

    fields = _data_fields(scope1)

    if not fields:
        return type(scope1)()

    if isinstance(scope1, TableScope) and isinstance(scope2, TableScope):
        common_dbs = sorted(set(scope1.dbs_to_tables) & set(scope2.dbs_to_tables))
        if not common_dbs:
            return None
        return TableScope(
            dbs_to_tables={
                db: sorted(set(scope1.dbs_to_tables[db]) & set(scope2.dbs_to_tables[db])) for db in common_dbs
            }
        )

    merged: dict[str, Any] = {}
    for name in fields:
        v1 = getattr(scope1, name)
        v2 = getattr(scope2, name)
        result = sorted(set(v1) & set(v2))
        if not result:
            return None
        merged[name] = result

    return type(scope1)(**merged)


def scope_union(scope1: ScopeDefinition, scope2: ScopeDefinition | None) -> ScopeDefinition:
    """Return the union of two scopes.

    Rules:
      - If scope2 is None, returns scope1.
      - Both scopes must be the same concrete type.
      - Scopes with no data fields (AllScope, CurrentUserScope) are returned as-is.
      - List fields are set-unioned.
      - TableScope unions the db keys and per-db table lists.
    """
    if scope2 is None:
        return scope1
    if type(scope1) is not type(scope2):
        raise ValueError("Cannot union scopes of different types")
    if isinstance(scope1, UnknownScope):
        raise TypeError("Cannot union unknown scopes")

    fields = _data_fields(scope1)

    if not fields:
        return type(scope1)()

    if isinstance(scope1, TableScope) and isinstance(scope2, TableScope):
        all_dbs = sorted(set(scope1.dbs_to_tables) | set(scope2.dbs_to_tables))
        return TableScope(
            dbs_to_tables={
                db: sorted(set(scope1.dbs_to_tables.get(db, [])) | set(scope2.dbs_to_tables.get(db, [])))
                for db in all_dbs
            }
        )

    merged: dict[str, Any] = {}
    for name in fields:
        v1 = getattr(scope1, name)
        v2 = getattr(scope2, name)
        merged[name] = sorted(set(v1) | set(v2))

    return type(scope1)(**merged)
