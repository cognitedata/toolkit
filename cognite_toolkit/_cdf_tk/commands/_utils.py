from collections.abc import Callable, Sequence

from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def _print_ids_or_length(resource_ids: Sequence[T_Identifier,], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"


def block_if_views_reference_containers(
    client: ToolkitClient,
    container_ids: Sequence[ContainerId],
    is_in_scope: Callable[[ViewId], bool],
) -> None:
    """Raise ToolkitValueError if any container is referenced by out-of-scope views.

    Calls the ``models/containers/inspect`` endpoint. For each container, views where
    ``is_in_scope`` returns True are excluded from blocking. Hidden views (those the caller
    lacks read access to) are always treated as out-of-scope.
    """
    if not container_ids:
        return

    blocked: dict[ContainerId, tuple[list[ViewId], int]] = {}
    for inspected in client.tool.containers.inspect(list(container_ids)):
        results = inspected.inspection_results
        out_of_scope_seen = [view for view in results.involved_views if not is_in_scope(view)]
        hidden_count = results.involved_view_count - len(results.involved_views)
        if not out_of_scope_seen and hidden_count == 0:
            continue
        container_id = ContainerId(space=inspected.space, external_id=inspected.external_id)
        blocked[container_id] = (out_of_scope_seen, hidden_count)
    if not blocked:
        return

    table = Table(
        title="Container deletion blocked: referenced by out-of-scope views",
        title_justify="left",
        show_lines=True,
    )
    table.add_column("Container", no_wrap=True)
    table.add_column("Referencing view", no_wrap=True)
    for container_id, (out_of_scope_seen, hidden_count) in blocked.items():
        container_label = f"{container_id.space}:{container_id.external_id}"
        rows: list[tuple[str, str]] = [
            (container_label if index == 0 else "", f"{view.space}:{view.external_id}/{view.version}")
            for index, view in enumerate(out_of_scope_seen)
        ]
        if hidden_count > 0:
            hidden_label = f"[dim italic]{hidden_count} view(s) you do not have access to[/]"
            rows.append(("" if out_of_scope_seen else container_label, hidden_label))
        for label, view_label in rows:
            table.add_row(label, view_label)
    print(table)
    raise ToolkitValueError(
        "Cannot proceed with the operation: one or more containers are referenced by views outside "
        "the current scope. Delete or move those views first, then re-run the operation."
    )
