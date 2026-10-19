from collections.abc import Iterable, Sequence
from typing import Literal, Protocol

from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataSetScope, ScopeDefinition, SpaceIDScope
from cognite_toolkit._cdf_tk.constants import DRY_RUN_ID


class DataSetItem(Protocol):
    data_set_id: int | None


def data_set_scope_from_ids(data_set_ids: Iterable[int | None]) -> ScopeDefinition:
    ids: set[int] = set()
    for data_set_id in data_set_ids:
        if data_set_id is None:
            return AllScope()
        ids.add(data_set_id)
    if not ids:
        return DataSetScope(ids=[])
    known_ids = {data_set_id for data_set_id in ids if data_set_id != DRY_RUN_ID}
    if not known_ids:
        # Dry-run placeholder IDs are not valid CDF dataset IDs (used when the dataset is only in the module).
        return DataSetScope(ids=[])
    return DataSetScope(ids=sorted(known_ids))


def dataset_scoped_resource(items: Sequence[DataSetItem]) -> ScopeDefinition:
    """Items must have a ``data_set_id: int | None`` attribute."""
    return data_set_scope_from_ids(item.data_set_id for item in items)


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
