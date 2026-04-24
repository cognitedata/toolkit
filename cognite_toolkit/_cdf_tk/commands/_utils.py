from collections.abc import Callable, Sequence

import questionary
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics
from rich import print
from rich.panel import Panel
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.constants import DMS_SOFT_DELETED_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def _print_ids_or_length(resource_ids: Sequence[T_Identifier,], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"


def validate_soft_delete_headroom(
    instance_statistics: InstanceStatistics,
    instances_to_soft_delete: int,
    *,
    action: str,
) -> None:
    """Abort if the operation would exhaust the soft-delete resource limit."""
    if instances_to_soft_delete <= 0:
        return
    used = instance_statistics.soft_deleted_instances
    limit = instance_statistics.soft_deleted_instances_limit
    margin = DMS_SOFT_DELETED_INSTANCE_LIMIT_MARGIN
    projected = used + instances_to_soft_delete
    headroom_after = limit - projected
    if headroom_after < margin:
        headroom_clause = (
            f"leaving only {headroom_after:,} instances of headroom, which is less than the required margin of {margin:,}."
            if headroom_after >= 0
            else f"exceeding the limit by {-headroom_after:,} instances."
        )
        raise ToolkitValueError(
            f"Cannot proceed with {action}, not enough soft-deleted instance capacity available. "
            f"Currently {used:,} of {limit:,} instances are soft-deleted. Performing this operation would add up to "
            f"{instances_to_soft_delete:,} more (projected total: {projected:,}), {headroom_clause} "
            f"Reduce what you delete, or wait for soft-deleted data to expire before retrying "
            f"(see: https://docs.cognite.com/cdf/dm/dm_concepts/dm_ingestion#soft-deletion for details)."
        )


def print_soft_delete_panel(
    instance_statistics: InstanceStatistics,
    instances_to_delete: int,
) -> None:
    """Print a warning panel about soft-delete resource limit impact."""
    used = max(0, instance_statistics.soft_deleted_instances)
    limit = instance_statistics.soft_deleted_instances_limit
    projected = used + instances_to_delete
    remaining_after = max(0, limit - projected)
    bar_width = 44

    bar = "".join(
        "[yellow]█[/yellow]"
        if (i + 0.5) / bar_width * limit < used
        else "[bright_magenta]█[/bright_magenta]"
        if (i + 0.5) / bar_width * limit < min(projected, limit)
        else "[dim]░[/dim]"
        for i in range(bar_width)
    )
    resource_usage_bar = (
        "[yellow]█[/yellow] [dim]already soft-deleted   [/dim]"
        "[bright_magenta]█[/bright_magenta] [dim]this operation   [/dim]"
        "[dim]░ remaining[/dim]\n\n"
        f"{bar}\n"
        f"[dim]Limit [/dim][bold]{limit:,}[/bold][dim]  ·  [/dim]"
        f"[yellow]{used:,}[/yellow][dim] + [/dim][bright_magenta]{instances_to_delete:,}[/bright_magenta]"
        f"[dim] → [/dim][bold]{projected:,}[/bold][dim] total soft-deleted (est.)  ·  [/dim]"
        f"[green]{remaining_after:,}[/green][dim] remaining[/dim]"
    )

    print(
        Panel(
            "By continuing this operation you will be deleting instances, which consumes your CDF project-wide "
            "[bold]soft-delete resource limit[/bold] for instances. If that resource limit is exhausted, you will "
            "not be able to delete any more instances until the soft-deleted data expires and is hard-deleted per "
            "the retention policy, which can take multiple days (see "
            "https://docs.cognite.com/cdf/dm/dm_concepts/dm_ingestion#soft-deletion for details).\n\n"
            f"[bold]This operation targets up to {instances_to_delete:,} instance(s).[/bold] Each deleted instance "
            f"counts toward the total soft-delete limit below.\n\n{resource_usage_bar}\n\n"
            "[bold]NOTE:[/bold] Please be aware, if you intended to delete containers or views, this does not "
            "require deleting instances. You can delete or change schema resources (containers, views, data models) "
            "without purging the instance data first. Only delete instances when you intend to remove specific "
            "data which was either ingested by error or is no longer needed or valid.",
            title="Deleting instances: Please acknowledge the following",
            title_align="left",
            border_style="yellow",
            expand=False,
        )
    )


def confirm_by_typing_project_name(message: str, client: ToolkitClient) -> bool:
    """Prompt the user to type the CDF project name to confirm a destructive operation."""
    client_project = client.config.project
    client.console.print(f"{message} in the CDF project [bold]{client_project!r}[/bold]")
    typed_project = questionary.text("To confirm, please type the name of the CDF project: ").unsafe_ask()
    if typed_project != client_project:
        client.console.print(
            f"The CDF project you typed does not match your credentials {typed_project!r}≠{client_project!r}. Exiting..."
        )
        return False
    return True


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
