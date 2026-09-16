import os
import sys
from datetime import datetime, timezone

import questionary
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.auth.cogidp import SessionProject, fetch_session_user_info
from cognite_toolkit._cdf_tk.commands.auth.data_classes import EnvironmentVariables, LoginFlow
from cognite_toolkit._cdf_tk.commands.auth.oidc import login_for_session, refresh_session_tokens, revoke_refresh_token
from cognite_toolkit._cdf_tk.commands.auth.session_store import StoredSession
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, SessionExpiredError


def confirm_login_flow_overrides_env(selected_flow: LoginFlow) -> bool:
    """Warn and confirm when login uses a different mode than .env."""
    env_flow = EnvironmentVariables.login_flow_from_environment()
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
    def login(self, org: str | None, force: bool, port: int | None) -> StoredSession | None:
        try:
            existing = StoredSession.load()
        except AuthenticationError:
            StoredSession.clear()
            existing = None

        if existing and not force:
            state = existing.token_state()
            if state != "EXPIRED":
                replace = questionary.confirm(
                    f'A session for organization "{existing.org}" already exists. Replace it?',
                    default=False,
                ).unsafe_ask()
                if not replace:
                    print("[yellow]Aborted.[/yellow]")
                    return None

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
        session.save()
        print("[green]Signed in.[/green]")
        return session

    def logout(self) -> None:
        try:
            metadata = StoredSession.load_metadata()
            if metadata is None:
                print("[yellow]No active session.[/yellow]")
                return
            session = StoredSession.load()
        except AuthenticationError:
            StoredSession.clear()
            print("[green]Session cleared.[/green]")
            return

        if session is not None:
            revoke_refresh_token(session.refresh_token, session.org)
        StoredSession.clear()
        print(f"[green]Signed out from organization {metadata.org}.[/green]")

    def status(self) -> None:
        try:
            session = StoredSession.ensure_fresh(refresh_session_tokens)
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

        current_project = os.environ.get("CDF_PROJECT", "").strip()

        print(f"\n[bold]Projects ({len(user_info.projects)}):[/bold]")
        for cluster, projects in sorted(by_cluster.items()):
            print(f"  [dim]{cluster}[/dim]")
            for project in projects:
                markers: list[str] = []
                if project.is_default:
                    markers.append("[green][default][/green]")
                if current_project and project.name == current_project:
                    markers.append("[green](current)[/green]")
                suffix = f" {' '.join(markers)}" if markers else ""
                print(f"    {project.name}{suffix}")
