import sys

import questionary
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.auth.data_classes import EnvironmentVariables, LoginFlow
from cognite_toolkit._cdf_tk.commands.auth.oidc import login_for_session, revoke_refresh_token
from cognite_toolkit._cdf_tk.commands.auth.session_store import StoredSession
from cognite_toolkit._cdf_tk.commands.auth.status_report import auth_status_from_runtime, render_auth_status
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError


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

    def status(self, verbose: bool = False, all_projects: bool = False, show_missing: bool = False) -> None:
        """Show whether you are authenticated, how, and which CDF projects you can access."""
        report, client = auth_status_from_runtime()
        render_auth_status(report, verbose=verbose, all_projects=all_projects, show_missing=show_missing, client=client)
