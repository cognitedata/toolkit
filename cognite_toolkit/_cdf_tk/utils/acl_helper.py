from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataSetScope, ScopeDefinition, SpaceIDScope


def dataset_scoped_resource(items: Sequence[Any]) -> ScopeDefinition:
    """Items must have a ``data_set_id: int | None`` attribute."""
    data_set_ids: set[int] = set()
    for item in items:
        if item.data_set_id is None:
            return AllScope()
        data_set_ids.add(item.data_set_id)
    return DataSetScope(ids=list(data_set_ids))


def space_scoped_resource(items: Sequence[Any]) -> ScopeDefinition:
    """Items must have a ``space: str`` attribute."""
    return SpaceIDScope(space_ids=sorted({item.space for item in items}))


def to_read_write_actions(actions: set[Literal["read", "write"]]) -> Any:
    """Convert lowercase read/write actions to uppercase READ/WRITE for ACLs."""
    return sorted(a.upper() for a in actions)
