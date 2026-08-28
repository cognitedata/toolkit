from datetime import datetime, timezone

import questionary
from rich import print

from cognite_toolkit._cdf_tk.auth.cogidp import fetch_session_user_info
from cognite_toolkit._cdf_tk.auth.oidc import login_for_session, revoke_refresh_token
from cognite_toolkit._cdf_tk.auth.session_refresh import SessionExpiredError, ensure_fresh_session
from cognite_toolkit._cdf_tk.auth.session_store import (
    clear_session,
    read_session_metadata,
    token_state,
    write_session,
)

from ._base import ToolkitCommand


class AuthSessionCommand(ToolkitCommand):
    def login(self, org: str | None, force: bool, port: int | None) -> None:
        existing = read_session_metadata()
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
        if existing:
            clear_session()

        if not org:
            org = questionary.text(
                "Enter your organization name",
                validate=lambda value: bool(value.strip()) or "Organization name is required",
            ).unsafe_ask()
            org = org.strip()

        session = login_for_session(org, port=port)
        write_session(session)
        print(f"\n[green]Signed in to organization {session.org}.[/green]")

    def logout(self) -> None:
        session = ensure_fresh_session()
        if session is None:
            metadata = read_session_metadata()
            if metadata is None:
                print("[yellow]No active session.[/yellow]")
                return
            clear_session()
            print("[green]Session cleared.[/green]")
            return
        revoke_refresh_token(session.refresh_token)
        clear_session()
        print(f"[green]Signed out from organization {session.org}.[/green]")

    def status(self) -> None:
        try:
            session = ensure_fresh_session()
        except SessionExpiredError as exc:
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

        by_cluster: dict[str, list] = {}
        for project in user_info.projects:
            cluster = project.cluster or "(unknown cluster)"
            by_cluster.setdefault(cluster, []).append(project)

        print(f"\n[bold]Projects ({len(user_info.projects)}):[/bold]")
        for cluster, projects in sorted(by_cluster.items()):
            print(f"  [dim]{cluster}[/dim]")
            for project in projects:
                marker = " [green][default][/green]" if project.is_default else ""
                print(f"    {project.name}{marker}")
