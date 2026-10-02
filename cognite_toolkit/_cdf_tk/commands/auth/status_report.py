"""Authentication status: what the token can do, separate from how it is printed."""

import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, cast

from rich.console import Console, Group, RenderableType
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from cognite_toolkit._cdf_tk import resource_ios
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, Scope, TableScope
from cognite_toolkit._cdf_tk.client.resource_classes.group.scope_logic import scope_intersection
from cognite_toolkit._cdf_tk.client.resource_classes.project import OrganizationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.token import FlatCapabilities
from cognite_toolkit._cdf_tk.commands.auth.data_classes import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.auth.data_classes._types import LoginFlow, Provider
from cognite_toolkit._cdf_tk.commands.auth.session_store import StoredSession
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ToolkitKeyError,
    ToolkitMissingValueError,
)
from cognite_toolkit._cdf_tk.resource_ios import AssetIO, GroupIO, RelationshipIO, ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._auth import ReplaceMethod
from cognite_toolkit._cdf_tk.utils import humanize_collection

DataModelingStatus = Literal["HYBRID", "DATA_MODELING_ONLY"]
ToolkitAction = Literal["READ", "WRITE"]

_MISSING_CREDENTIALS = "No CDF credentials are configured. Run `cdf auth login` or `cdf auth init`."

_METHOD_LABELS: dict[LoginFlow, str] = {
    "session": "Session",
    "client_credentials": "Service principal",
    "device_code": "Device code",
    "interactive": "Interactive",
    "token": "Token",
}

_PROVIDER_LABELS: dict[Provider, str] = {
    "entra_id": "Microsoft Entra ID",
    "auth0": "Auth0",
    "cdf": "Cognite IDP",
    "other": "Other",
}

_SCOPE_VALUE_LIMIT = 6


@dataclass(frozen=True)
class IdentityProvider:
    name: str
    tenant: str | None
    access_claims: tuple[str, ...]
    source: Literal["project", "environment"]


@dataclass(frozen=True)
class SessionDetails:
    organization: str
    access_token_expires_at: str
    refresh_token_expires_at: str
    state: str


@dataclass(frozen=True)
class AclScopeGrant:
    acl_name: str
    actions: tuple[str, ...]
    scope: Scope


@dataclass
class ActionAccess:
    applicable: bool
    grants: list[AclScopeGrant]
    missing: list[str]

    @property
    def granted(self) -> bool:
        return self.applicable and not self.missing and bool(self.grants)


@dataclass(frozen=True)
class ResourceAccess:
    io_name: str
    kind: str
    folder_name: str
    read: ActionAccess
    write: ActionAccess


@dataclass(frozen=True)
class MergedCapability:
    acl_name: str
    actions: tuple[str, ...]
    scope: Scope


@dataclass
class ProjectAccess:
    name: str
    is_current: bool
    capability_count: int
    group_count: int
    data_modeling_status: DataModelingStatus | None
    capabilities: list[MergedCapability]
    resources: list[ResourceAccess]
    resource_types_checked: int


@dataclass
class AuthStatus:
    authenticated: bool
    failure: str | None
    method_label: str
    identity_provider: IdentityProvider | None
    subject: str | None
    cluster: str | None
    current_project: str | None
    projects: list[ProjectAccess]
    session: SessionDetails | None


def _load_environment() -> tuple[EnvironmentVariables | None, Exception | None]:
    """Read CDF credentials from the environment. Returns an error message when they are missing or invalid."""
    try:
        return EnvironmentVariables.create_from_environment(), None
    except (ToolkitMissingValueError, AuthenticationError) as exc:
        return None, exc


def auth_status_from_runtime() -> tuple[AuthStatus, ToolkitClient | None]:
    """Load credentials from the environment and describe the resulting CDF access.

    The client is returned when authentication succeeded, so scope IDs can be shown as external IDs.
    """
    environment, error = _load_environment()
    client: ToolkitClient | None = None
    if error is None and environment is not None:
        try:
            client = environment.get_client()
        except (AuthenticationError, ToolkitMissingValueError, ToolkitKeyError) as exc:
            error = exc
            client = None
    status = collect_auth_status(client=client, environment=environment, failure=error)
    if not status.authenticated:
        return status, None
    return status, client


def collect_auth_status(
    client: ToolkitClient | None = None,
    environment: EnvironmentVariables | None = None,
    failure: Exception | None = None,
) -> AuthStatus:
    """Build the authentication report from the Toolkit client, environment, and any error that occurred
    while loading credentials."""
    if failure is not None:
        return _unauthenticated(environment, failure)

    if client is None:
        if environment is None:
            return _unauthenticated(None, _MISSING_CREDENTIALS)
        try:
            client = environment.get_client()
        except (AuthenticationError, ToolkitMissingValueError, ToolkitKeyError) as exc:
            return _unauthenticated(environment, exc)

    try:
        inspected = client.tool.token.inspect()
    except (ToolkitAPIError, AuthenticationError, AuthorizationError) as exc:
        return _unauthenticated(environment, exc)

    current_project = client.config.project
    identity_provider = _identity_provider(client, environment)
    status_by_project = _data_modeling_by_project(client)
    projects = [
        _project_access(
            inspected.to_project_capabilities(project.project_url_name),
            name=project.project_url_name,
            group_count=len(project.groups),
            is_current=project.project_url_name == current_project,
            data_modeling_status=status_by_project.get(project.project_url_name),
        )
        for project in inspected.projects
    ]
    projects.sort(key=lambda project: (not project.is_current, project.name.casefold()))
    return AuthStatus(
        authenticated=True,
        failure=None,
        method_label=_method_label(environment),
        identity_provider=identity_provider,
        subject=inspected.subject or None,
        cluster=_cluster_name(client),
        current_project=current_project,
        projects=projects,
        session=_session_details(environment),
    )


def _describe_identity_provider(organization: OrganizationResponse) -> IdentityProvider | None:
    """Name the identity provider configured on the CDF project."""
    oidc = organization.oidc_configuration
    if oidc is None:
        return None
    token_url = oidc.token_url or oidc.issuer
    if not token_url:
        return None
    parsed = urllib.parse.urlparse(token_url)
    hostname = (parsed.hostname or "").lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    name, tenant = _provider_from_hostname(hostname, path_parts)
    claims = tuple(claim.claim_name for claim in oidc.access_claims)
    return IdentityProvider(name=name, tenant=tenant, access_claims=claims, source="project")


def merged_capability_rows(capabilities: FlatCapabilities) -> list[MergedCapability]:
    """Group a flat capability map into one row per ACL and scope."""
    grouped: list[MergedCapability] = []
    index: dict[tuple[str, str], list[str]] = {}
    order: list[tuple[str, str]] = []
    scopes: dict[tuple[str, str], Scope] = {}
    for (_, acl_name, action), scope in capabilities.items():
        key = (acl_name, scope.model_dump_json())
        if key not in index:
            index[key] = []
            order.append(key)
            scopes[key] = scope
        index[key].append(action)
    for key in order:
        grouped.append(MergedCapability(acl_name=key[0], actions=tuple(sorted(index[key])), scope=scopes[key]))
    grouped.sort(key=lambda row: (row.acl_name, row.actions))
    return grouped


def resources_from_capabilities(
    capabilities: FlatCapabilities,
    data_modeling_status: DataModelingStatus | None,
) -> tuple[list[ResourceAccess], int]:
    """Map merged capabilities onto toolkit resource types and the scope of that access.

    Assets and relationships are omitted on DATA_MODELING_ONLY projects, matching auth verify.
    Every checked type is returned, including types the identity cannot read or write.
    """
    excluded = {AssetIO, RelationshipIO} if data_modeling_status == "DATA_MODELING_ONLY" else set()
    resources: list[ResourceAccess] = []
    checked = 0
    # data_models is an alias of data_modeling, so RESOURCE_CRUD_LIST contains those classes twice.
    seen: set[type[ResourceIO]] = set()
    for io_cls in resource_ios.RESOURCE_IO_LIST:
        if io_cls in seen or io_cls in excluded:
            continue
        seen.add(io_cls)
        read = resolve_action_access(io_cls, capabilities, "READ")
        write = resolve_action_access(io_cls, capabilities, "WRITE")
        if not read.applicable and not write.applicable:
            continue
        checked += 1
        resources.append(
            ResourceAccess(
                io_name=io_cls.__name__,
                kind=io_cls.kind,
                folder_name=io_cls.folder_name,
                read=read,
                write=write,
            )
        )
    resources.sort(key=lambda item: (item.folder_name, item.kind, item.io_name))
    return resources, checked


def resolve_action_access(
    io_cls: type[ResourceIO],
    capabilities: FlatCapabilities,
    action: ToolkitAction,
) -> ActionAccess:
    """Decide whether a resource type's READ or WRITE ACLs are covered, and at which scope."""
    required = list(io_cls.create_acl({action}, AllScope()))
    if not required:
        # The ResourceIO class does not have any required ACLs, typically this is for child resources such as
        # TransformationSchedule that assumes we check TransformationIO instead.
        return ActionAccess(applicable=False, grants=[], missing=[])

    grants: list[AclScopeGrant] = []
    missing: list[str] = []
    for acl in required:
        found_actions: list[str] = []
        found_scopes: list[Scope] = []
        acl_missing: list[str] = []
        for acl_action in acl.actions:
            scope = capabilities.get((type(acl), acl.acl_name, acl_action))
            if scope is None:
                acl_missing.append(acl_action)
                continue
            found_actions.append(acl_action)
            found_scopes.append(scope)
        if acl_missing:
            missing.extend(f"{acl.acl_name} {acl_action}" for acl_action in acl_missing)
            continue
        unified = _unify_scopes(found_scopes)
        if unified is None:
            missing.append(f"{acl.acl_name} (scopes do not overlap)")
            continue
        grants.append(AclScopeGrant(acl_name=acl.acl_name, actions=tuple(found_actions), scope=unified))
    return ActionAccess(applicable=True, grants=grants, missing=missing)


def render_auth_status(
    status: AuthStatus,
    verbose: bool = False,
    all_projects: bool = False,
    show_missing: bool = False,
    client: ToolkitClient | None = None,
    console: Console | None = None,
) -> None:
    """Print an authentication report. All wording and layout lives here."""
    console = console or Console(highlight=False)
    if not status.authenticated:
        console.print(_unauthenticated_panel(status))
        return

    console.print(_authentication_panel(status))
    if status.current_project and not any(project.is_current for project in status.projects):
        console.print(
            f"\n[yellow]The current project {escape(status.current_project)} "
            "is not in the projects you can access.[/yellow]"
        )
    console.print()
    console.print(_projects_table(status.projects))
    if not verbose:
        console.print(
            "\n[dim]Run with --verbose to list capabilities and toolkit resources for the current project. "
            "Add --all to include every project, and --show-missing to include resources you cannot access.[/dim]"
        )
        return

    detailed = _projects_to_detail(status, all_projects)
    if not detailed:
        if not all_projects:
            console.print("\n[dim]Pass --all to list capabilities and toolkit resources for every project.[/dim]")
        return
    if not all_projects:
        console.print("\n[dim]Showing the current project. Pass --all to include every project.[/dim]")
    console.print()
    for project in detailed:
        console.print(_verbose_project(project, show_missing=show_missing, client=client))
        console.print()


def _unauthenticated(environment: EnvironmentVariables | None, failure: Exception | str) -> AuthStatus:
    return AuthStatus(
        authenticated=False,
        failure=_failure_message(failure),
        method_label=_method_label(environment),
        identity_provider=_identity_provider_from_environment(environment) if environment is not None else None,
        subject=None,
        cluster=environment.CDF_CLUSTER if environment is not None else None,
        current_project=environment.CDF_PROJECT if environment is not None else None,
        projects=[],
        session=_session_details(environment),
    )


def _failure_message(failure: Exception | str | None) -> str:
    if failure is None:
        return "The credentials were rejected."
    message = str(failure).strip() or "The credentials were rejected."
    if message.lower().startswith("not ") or "cdf auth" in message:
        return message
    return f"Not authenticated. {message}"


def _method_label(environment: EnvironmentVariables | None) -> str:
    if environment is None:
        return "Unknown"
    return _METHOD_LABELS.get(environment.LOGIN_FLOW, environment.LOGIN_FLOW)


def _identity_provider(client: ToolkitClient, environment: EnvironmentVariables | None) -> IdentityProvider | None:
    try:
        described = _describe_identity_provider(client.project.organization())
    except (ToolkitAPIError, AuthorizationError):
        described = None
    if described is not None:
        return described
    return _identity_provider_from_environment(environment)


def _identity_provider_from_environment(environment: EnvironmentVariables | None) -> IdentityProvider | None:
    if environment is None:
        return None
    return IdentityProvider(
        name=_PROVIDER_LABELS.get(environment.PROVIDER, environment.PROVIDER),
        tenant=environment.IDP_TENANT_ID,
        access_claims=(),
        source="environment",
    )


def _provider_from_hostname(hostname: str, path_parts: list[str]) -> tuple[str, str | None]:
    if _is_entra_host(hostname):
        tenant = path_parts[0] if path_parts and path_parts[0] != "oauth2" else None
        return "Microsoft Entra ID", tenant
    if hostname == "auth0.com" or hostname.endswith(".auth0.com"):
        tenant = hostname.split(".")[0] if hostname != "auth0.com" else None
        return "Auth0", tenant
    if hostname == "auth.cognite.com" or hostname.endswith(".auth.cognite.com"):
        return "Cognite IDP", None
    return hostname or "Unknown", None


def _is_entra_host(hostname: str) -> bool:
    entra_hosts = (
        "login.windows.net",
        "login.microsoftonline.com",
        "sts.windows.net",
    )
    return hostname in entra_hosts or hostname.endswith(tuple(f".{host}" for host in entra_hosts))


def _data_modeling_by_project(client: ToolkitClient) -> dict[str, DataModelingStatus]:
    try:
        statuses = client.project.status()
    except (ToolkitAPIError, AuthorizationError):
        return {}
    return {item.url_name: item.data_modeling_status for item in statuses}


def _project_access(
    capabilities: FlatCapabilities,
    name: str,
    group_count: int,
    is_current: bool,
    data_modeling_status: DataModelingStatus | None,
) -> ProjectAccess:
    merged = merged_capability_rows(capabilities)
    resources, checked = resources_from_capabilities(capabilities, data_modeling_status)
    return ProjectAccess(
        name=name,
        is_current=is_current,
        capability_count=len(merged),
        group_count=group_count,
        data_modeling_status=data_modeling_status,
        capabilities=merged,
        resources=resources,
        resource_types_checked=checked,
    )


def _session_details(environment: EnvironmentVariables | None) -> SessionDetails | None:
    if environment is None or environment.LOGIN_FLOW != "session":
        return None
    try:
        metadata = StoredSession.load_metadata()
    except AuthenticationError:
        return None
    if metadata is None:
        return None
    return SessionDetails(
        organization=metadata.org,
        access_token_expires_at=metadata.access_token_expires_at,
        refresh_token_expires_at=metadata.refresh_token_expires_at,
        state=metadata.token_state(),
    )


def _cluster_name(client: ToolkitClient) -> str | None:
    for attribute in ("cluster", "cdf_cluster"):
        value = getattr(client.config, attribute, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    base_url = getattr(client.config, "base_url", None)
    if not isinstance(base_url, str):
        return None
    hostname = urllib.parse.urlparse(base_url).hostname or ""
    if hostname.endswith(".cognitedata.com"):
        return hostname.removesuffix(".cognitedata.com")
    return hostname or None


def _unify_scopes(scopes: list[Scope]) -> Scope | None:
    if not scopes:
        return None
    if len(scopes) == 1:
        return scopes[0]
    try:
        unified = scope_intersection(*scopes)
    except (TypeError, ValueError):
        return None
    if unified is None:
        return None
    return cast(Scope, unified)


def _unauthenticated_panel(status: AuthStatus) -> Panel:
    rows: list[tuple[str, str]] = [("Authenticated", "[red]No[/red]")]
    if status.failure:
        rows.append(("Reason", escape(status.failure)))
    _append_method_rows(rows, status)
    if "cdf auth" not in (status.failure or ""):
        rows.append(("Next step", "Run [bold]cdf auth login[/bold] or [bold]cdf auth init[/bold]."))
    return Panel(_key_values(rows), title="Authentication", border_style="red", expand=False)


def _authentication_panel(status: AuthStatus) -> Panel:
    rows: list[tuple[str, str]] = [("Authenticated", "[green]Yes[/green]"), ("Method", escape(status.method_label))]
    _append_identity_rows(rows, status.identity_provider)
    if status.subject:
        rows.append(("Subject", escape(status.subject)))
    if status.cluster:
        rows.append(("Cluster", escape(status.cluster)))
    if status.current_project:
        rows.append(("Project", escape(status.current_project)))
    if status.session is not None:
        rows.append(("Organization", escape(status.session.organization)))
        rows.append(("Access token expires", _expiry(status.session.access_token_expires_at, status.session.state)))
        rows.append(("Refresh token expires", _expiry(status.session.refresh_token_expires_at, "VALID")))
    return Panel(_key_values(rows), title="Authentication", border_style="green", expand=False)


def _append_method_rows(rows: list[tuple[str, str]], status: AuthStatus) -> None:
    if status.method_label != "Unknown":
        rows.append(("Method", escape(status.method_label)))
    if status.current_project:
        rows.append(("Project", escape(status.current_project)))
    if status.cluster:
        rows.append(("Cluster", escape(status.cluster)))
    if status.identity_provider is not None:
        _append_identity_rows(rows, status.identity_provider)


def _append_identity_rows(rows: list[tuple[str, str]], provider: IdentityProvider | None) -> None:
    if provider is None:
        rows.append(("Identity provider", "[dim]Unknown[/dim]"))
        return
    name = escape(provider.name)
    if provider.source == "environment":
        name += " [dim](from environment)[/dim]"
    rows.append(("Identity provider", name))
    if provider.tenant:
        rows.append(("Tenant", escape(provider.tenant)))
    if provider.access_claims:
        rows.append(("Access claims", escape(humanize_collection(provider.access_claims))))


def _projects_table(projects: list[ProjectAccess]) -> Table:
    table = Table(
        title="Projects you can access",
        caption="Capabilities are merged by ACL and scope. Groups are the groups you belong to in that project.",
        caption_style="dim",
        expand=False,
    )
    table.add_column("Project")
    table.add_column("Capabilities", justify="right")
    table.add_column("Data modeling")
    table.add_column("Groups", justify="right")
    if not projects:
        table.add_row("[dim]None[/dim]", "—", "—", "—")
        return table
    for project in projects:
        name = escape(project.name)
        if project.is_current:
            name = f"[bold]{name}[/bold] [dim]current[/dim]"
        table.add_row(
            name,
            str(project.capability_count),
            _status_markup(project.data_modeling_status),
            str(project.group_count),
        )
    return table


def _verbose_project(
    project: ProjectAccess, show_missing: bool = False, client: ToolkitClient | None = None
) -> RenderableType:
    title = Text(project.name)
    if project.is_current:
        title.append("  current", style="dim")
    lookups = _scope_lookups(client)
    return Group(
        Rule(title, style="cyan" if project.is_current else "white", align="left"),
        _capability_table(project, lookups),
        _resource_table(project, show_missing=show_missing, lookups=lookups),
    )


def _capability_table(
    project: ProjectAccess,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None = None,
) -> Table:
    table = Table(title="Capabilities", expand=False)
    table.add_column("Capability")
    table.add_column("Actions")
    table.add_column("Scope")
    if not project.capabilities:
        table.add_row("[dim]None[/dim]", "—", "—")
        return table
    for capability in project.capabilities:
        table.add_row(
            escape(capability.acl_name),
            escape(", ".join(capability.actions)),
            _paint_scope(capability.scope, capability.acl_name, lookups),
        )
    return table


def _resource_table(
    project: ProjectAccess,
    show_missing: bool = False,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None = None,
) -> Table:
    resources = project.resources
    if not show_missing:
        resources = [resource for resource in resources if resource.read.granted or resource.write.granted]
    caption = None if show_missing else f"{len(resources)} of {project.resource_types_checked} toolkit resource types"
    table = Table(title="Toolkit resources", caption=caption, caption_style="dim", expand=False)
    table.add_column("Resource", overflow="fold")
    table.add_column("Folder", overflow="fold")
    table.add_column("Read", overflow="fold")
    table.add_column("Write", overflow="fold")
    if not resources:
        table.add_row("[dim]None[/dim]", "—", "—", "—")
        return table
    for resource in resources:
        table.add_row(
            escape(resource_label(resource.io_name)),
            escape(resource.folder_name),
            _paint_access(format_action_access(resource.read, lookups)),
            _paint_access(format_action_access(resource.write, lookups)),
        )
    return table


def resource_label(io_name: str) -> str:
    """Drop the IO suffix so the resource table shows Asset rather than AssetIO."""
    if io_name.endswith("IO"):
        return io_name.removesuffix("IO")
    return io_name


def _projects_to_detail(status: AuthStatus, all_projects: bool) -> list[ProjectAccess]:
    if all_projects:
        return status.projects
    return [project for project in status.projects if project.is_current]


def format_scope(
    scope: Scope,
    acl_name: str | None = None,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None = None,
) -> str:
    if isinstance(scope, AllScope):
        return "all"
    if isinstance(scope, TableScope):
        parts: list[str] = []
        items = list(scope.dbs_to_tables.items())
        for database, tables in items[:4]:
            shown = tables[:_SCOPE_VALUE_LIMIT]
            body = ", ".join(shown)
            if len(tables) > len(shown):
                body += f", +{len(tables) - len(shown)} more"
            parts.append(f"{database}: {body}")
        if len(items) > 4:
            parts.append(f"+{len(items) - 4} more")
        return "tableScope {" + "; ".join(parts) + "}"
    payload = _scope_payload(scope, acl_name, lookups)
    if not payload:
        return scope.scope_name
    if len(payload) == 1:
        return f"{scope.scope_name} {_format_scope_value(next(iter(payload.values())))}"
    rendered = ", ".join(f"{key}={_format_scope_value(value)}" for key, value in payload.items())
    return f"{scope.scope_name} ({rendered})"


def format_action_access(
    access: ActionAccess,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None = None,
) -> str:
    if not access.applicable:
        return "—"
    if not access.granted:
        return "No access"
    if len(access.grants) == 1:
        grant = access.grants[0]
        return format_scope(grant.scope, grant.acl_name, lookups)
    return " | ".join(
        f"{grant.acl_name} {format_scope(grant.scope, grant.acl_name, lookups)}" for grant in access.grants
    )


def _scope_lookups(client: ToolkitClient | None) -> dict[tuple[str, str] | str, ReplaceMethod] | None:
    """Reuse GroupIO's map from ACL and scope to the lookup that turns an internal ID into an external ID."""
    if client is None:
        return None
    return GroupIO(client).create_replace_method_by_acl_and_scope()


def _scope_payload(
    scope: Scope,
    acl_name: str | None,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None,
) -> dict[str, Any]:
    # by_alias so field names match ReplaceMethod.id_name from GroupIO ("ids", "rootIds").
    payload = scope.model_dump(by_alias=True, exclude={"scope_name"}, exclude_none=True)
    if lookups is None:
        return payload
    if acl_name is not None and (method := lookups.get((acl_name, scope.scope_name))) is not None:
        replace: ReplaceMethod | None = method
    else:
        replace = lookups.get(scope.scope_name)
    if replace is None:
        return payload
    field_name = replace.id_name
    ids = payload.get(field_name)
    if not isinstance(ids, list):
        return payload
    payload[field_name] = [_external_id_label(replace.reverse_lookup_method, id_) for id_ in ids]
    return payload


def _external_id_label(lookup: Callable[[int], str | None], id_: object) -> object:
    if not isinstance(id_, int):
        return id_
    try:
        external_id = lookup(id_)
    except (ToolkitAPIError, AuthorizationError, AuthenticationError):
        return id_
    if not external_id:
        return id_
    return external_id


def _format_scope_value(value: object) -> str:
    if isinstance(value, list):
        shown = [str(item) for item in value[:_SCOPE_VALUE_LIMIT]]
        body = ", ".join(shown)
        extra = len(value) - len(shown)
        if extra:
            body += f", +{extra} more"
        return f"[{body}]"
    if isinstance(value, dict):
        return str(value)
    return str(value)


def _paint_scope(
    scope: Scope,
    acl_name: str | None = None,
    lookups: dict[tuple[str, str] | str, ReplaceMethod] | None = None,
) -> str:
    label = format_scope(scope, acl_name, lookups)
    if label == "all":
        return "[green]all[/green]"
    return escape(label)


def _paint_access(label: str) -> str:
    if label == "all":
        return "[green]all[/green]"
    if label == "—":
        return "[dim]—[/dim]"
    if label == "No access" or label.startswith("Missing "):
        return f"[red]{escape(label)}[/red]"
    return escape(label)


def _status_markup(status: DataModelingStatus | None) -> str:
    if status == "HYBRID":
        return "[cyan]HYBRID[/cyan]"
    if status == "DATA_MODELING_ONLY":
        return "[yellow]DATA_MODELING_ONLY[/yellow]"
    return "[dim]unknown[/dim]"


def _expiry(value: str, state: str) -> str:
    label = escape(_format_timestamp(value))
    if state == "EXPIRING":
        return f"{label}  [yellow]expiring soon[/yellow]"
    if state == "EXPIRED":
        return f"{label}  [red]expired[/red]"
    return label


def _format_timestamp(value: str) -> str:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _key_values(rows: list[tuple[str, str]]) -> Table:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold", no_wrap=True)
    table.add_column()
    for label, value in rows:
        table.add_row(label, value)
    return table
