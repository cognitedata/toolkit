import sys
from datetime import datetime, timezone

import questionary
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError
from cognite_toolkit._cdf_tk.utils.auth import LoginFlow, read_env_login_flow

from .cogidp import SessionProject, fetch_session_user_info
from .oidc import login_for_session, revoke_refresh_token
from .session_keyring import read_session_token
from .session_refresh import SessionExpiredError, ensure_fresh_session
from .session_store import (
    clear_org_tokens,
    clear_session,
    read_session_metadata,
    token_state,
    write_session,
)


def confirm_login_flow_overrides_env(selected_flow: LoginFlow) -> bool:
    """Warn and confirm when login uses a different mode than .env."""
    env_flow = read_env_login_flow()
    if env_flow is None or env_flow == selected_flow:
        return True

    print(
        "[yellow]Your .env configures "
        f"LOGIN_FLOW={env_flow!r}, but you selected {selected_flow!r}. "
        "Continuing will switch the toolkit to the new authentication mode.[/yellow]"
    )
    if not sys.stdin.isatty():
        raise AuthenticationError(
            f".env uses LOGIN_FLOW={env_flow!r}, but login was requested with {selected_flow!r}. "
            "Update .env or run in an interactive terminal."
        )
    return questionary.confirm("Do you want to continue?", default=False).unsafe_ask()


class AuthSessionCommand(ToolkitCommand):
    def login(self, org: str | None, force: bool, port: int | None) -> None:
        try:
            existing = read_session_metadata()
        except AuthenticationError:
            clear_session()
            existing = None

        if existing and not force:
            state = token_state(existing)
            if state != "EXPIRED":
                replace = questionary.confirm(
                    f'A session for organization "{existing.org}" already exists. Replace it?',
                    default=False,
                ).unsafe_ask()
                if not replace:
                    print("[yellow]Aborted.[/yellow]")
                    return

        if not org:
            if not sys.stdin.isatty():
                raise AuthenticationError(
                    "Organization name is required. Pass --org when running without an interactive terminal."
                )
            org = questionary.text(
                "Enter your organization name",
                validate=lambda value: bool(value.strip()) or "Organization name is required",
            ).unsafe_ask()
            org = org.strip()

        session = login_for_session(org, port=port)
        if existing and existing.org != session.org:
            clear_org_tokens(existing.org)
        write_session(session)
        print("[green]Signed in.[/green]")

    def logout(self) -> None:
        try:
            metadata = read_session_metadata()
        except AuthenticationError:
            clear_session()
            print("[green]Session cleared.[/green]")
            return

        if metadata is None:
            print("[yellow]No active session.[/yellow]")
            return

        refresh_token = read_session_token(f"{metadata.org}/refreshToken")
        if refresh_token:
            revoke_refresh_token(refresh_token, metadata.org)
        clear_session()
        print(f"[green]Signed out from organization {metadata.org}.[/green]")

    def status(self) -> None:
        try:
            session = ensure_fresh_session()
        except (SessionExpiredError, AuthenticationError) as exc:
            print(f"[red]{exc}[/red]")
            return
        if session is None:
            print("[yellow]Not signed in. Run `cdf auth login` to sign in.[/yellow]")
            return

        user_info = fetch_session_user_info(session.org, session.access_token)
        display_name = user_info.email or user_info.preferred_username or user_info.name or user_info.sub
        access_expires = datetime.fromisoformat(session.access_token_expires_at.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
        refresh_expires = datetime.fromisoformat(session.refresh_token_expires_at.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )

        print(f"\n[bold]Organization:[/bold] {session.org}")
        print(f"[bold]User:[/bold] {display_name}")
        print(f"[bold]Access token expires:[/bold] {access_expires.isoformat()}")
        print(f"[bold]Refresh token expires:[/bold] {refresh_expires.isoformat()}")

        if not user_info.projects:
            print("\n[dim]No projects returned from CogIdP.[/dim]")
            return

        by_cluster: dict[str, list[SessionProject]] = {}
        for project in user_info.projects:
            cluster = project.cluster or "(unknown cluster)"
            by_cluster.setdefault(cluster, []).append(project)

        print(f"\n[bold]Projects ({len(user_info.projects)}):[/bold]")
        for cluster, projects in sorted(by_cluster.items()):
            print(f"  [dim]{cluster}[/dim]")
            for project in projects:
                marker = " [green][default][/green]" if project.is_default else ""
                print(f"    {project.name}{marker}")
