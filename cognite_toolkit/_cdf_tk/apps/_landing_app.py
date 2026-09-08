import contextlib
from pathlib import Path
from typing import Annotated, Any

import typer

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import AuthCommand, InitCommand, ModulesCommand, RepoCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class InitApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if Flags.V09.is_enabled():
            self.callback(invoke_without_command=True)(self.main)
            self.command("auth")(self.auth)
            self.command("access")(self.access)
            self.command("modules")(self.modules)
            self.command("repo")(self.repo)

    def main(
        self,
        ctx: typer.Context,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
    ) -> None:
        """Getting started checklist."""
        if ctx.invoked_subcommand is not None:
            return
        self._run_checklist(dry_run=dry_run)

    def auth(
        self,
        reset: Annotated[
            bool,
            typer.Option(
                "--reset",
                help="Start over from scratch, ignoring any existing environment variables. "
                "An existing .env file will be backed up before being overwritten.",
                hidden=True,
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Turn on to get more verbose output when running the command"),
        ] = False,
    ) -> None:
        """Configure project credentials in .env (OIDC / service principal)."""
        cmd = AuthCommand()
        cmd.run(lambda: cmd.init(reset=reset))

    def access(
        self,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run. This means that no changes to CDF will be made.",
            ),
        ] = False,
        no_prompt: Annotated[
            bool,
            typer.Option(
                "--no-prompt",
                "-np",
                help="Whether to skip prompts. Useful for CI/CD pipelines.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Turn on to get more verbose output when running the command"),
        ] = False,
    ) -> None:
        """Set up toolkit group and grant capabilities."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = AuthCommand(client=client)
        cmd.run(lambda: cmd.provision_access(client, dry_run=dry_run, no_prompt=no_prompt))

    def modules(
        self,
        organization_dir: Annotated[
            Path | None,
            typer.Argument(help="Directory path to project to initialize or upgrade with templates."),
        ] = None,
        all: Annotated[bool, typer.Option("--all", help="Copy all available templates.")] = False,
        clean: Annotated[bool, typer.Option("--clean", "-a", help="Clean target directory if it exists")] = False,
        library_url: Annotated[
            str | None,
            typer.Option("--library-url", "-u", help="URL of the library to add to the project."),
        ] = None,
        library_checksum: Annotated[
            str | None,
            typer.Option(
                "--library-checksum",
                "-c",
                help="Library zip checksum (optional; accepted for compatibility, not verified).",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Turn on to get more verbose output when running the command"),
        ] = False,
    ) -> None:
        """Initialize or upgrade a CDF project with module templates."""
        client: ToolkitClient | None = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            client = EnvironmentVariables.create_from_environment().get_client()

        with ModulesCommand(client=client) as cmd:
            cmd.run(
                lambda: cmd.init(
                    organization_dir=organization_dir,
                    select_all=all,
                    clean=clean,
                    library_url=library_url,
                    library_checksum=library_checksum,
                )
            )

    def repo(
        self,
        cwd: Annotated[Path, typer.Argument(help="")] = Path.cwd(),
        host: str | None = typer.Option(
            None,
            "--host",
            "-h",
            help="Hosting service for the repository. If not provided, you will be prompted to choose.",
        ),
        verbose: bool = typer.Option(False, "-v", "--verbose", help="Verbose output"),
    ) -> None:
        """Initialize a new git repository with files like .gitignore and workflows."""
        client: ToolkitClient | None = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            client = EnvironmentVariables.create_from_environment().get_client()
        cmd = RepoCommand(client=client)
        cmd.run(lambda: cmd.init(cwd=cwd, host=host, verbose=verbose))

    def _run_checklist(self, dry_run: bool = False) -> None:
        client: ToolkitClient | None = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            # Remove the Error message from failing to load the config
            # This is verified in check_auth
            client = EnvironmentVariables.create_from_environment().get_client()

        cmd = InitCommand(client=client)
        # Tracking  command with the usual lambda run construct
        # is intentionally left out because we don't want to expose the user to the warning
        # before they've had the chance to opt in (which is something they'll do later using this command).
        cmd.execute(dry_run=dry_run)


# Backwards compatibility for non-v09 registration
class LandingApp(InitApp):
    def main_init(
        self,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
    ) -> None:
        """Getting started checklist"""
        self._run_checklist(dry_run=dry_run)
