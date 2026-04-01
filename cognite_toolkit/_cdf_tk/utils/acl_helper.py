from collections.abc import Sequence
from typing import Literal, Protocol

from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataSetScope, ScopeDefinition, SpaceIDScope


class DataSetItem(Protocol):
    data_set_id: int | None


def dataset_scoped_resource(items: Sequence[DataSetItem]) -> ScopeDefinition:
    """Items must have a ``data_set_id: int | None`` attribute."""
    data_set_ids: set[int] = set()
    for item in items:
        if item.data_set_id is None:
            return AllScope()
        data_set_ids.add(item.data_set_id)
    return DataSetScope(ids=list(data_set_ids))


class SpaceItem(Protocol):
    space: str


def space_scoped_resource(items: Sequence[SpaceItem]) -> ScopeDefinition:
    """Items must have a ``space: str`` attribute."""
    return SpaceIDScope(space_ids=sorted({item.space for item in items}))


def as_read_create_update_delete_actions(
    actions: set[Literal["READ", "WRITE"]],
) -> list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]:
    acl_actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]] = []
    if "READ" in actions:
        acl_actions.append("READ")
    if "WRITE" in actions:
        acl_actions.extend(["CREATE", "UPDATE", "DELETE"])
    return acl_actions


def as_instance_acl_actions(
    actions: set[Literal["READ", "WRITE"]],
) -> list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]:
    acl_actions: list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]] = []
    if "READ" in actions:
        acl_actions.append("READ")
    if "WRITE" in actions:
        acl_actions.append("WRITE")
    return acl_actions


def as_read_list_write_actions(
    actions: set[Literal["READ", "WRITE"]],
) -> list[Literal["READ", "WRITE", "LIST"]]:
    acl_actions: list[Literal["READ", "WRITE", "LIST"]] = []
    if "READ" in actions:
        acl_actions.extend(["READ", "LIST"])
    if "WRITE" in actions:
        acl_actions.append("WRITE")
    return acl_actions
