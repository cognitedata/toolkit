import sys

import typer
from rich import print

from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables


def print_help_if_no_subcommand(ctx: typer.Context) -> None:
    """Print this group's help and exit when no subcommand was given."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()


def warn_if_missing_credentials(ctx: typer.Context) -> None:
    """Warn if no .env file was loaded and the required environment variables are missing.

    The root callback stores whether a .env file was found on ctx.obj. Click passes
    ctx.obj on to sub-contexts, so command group callbacks can call this as well.
    """
    if getattr(ctx.obj, "env_file_found", False):
        return
    try:
        env_vars = EnvironmentVariables.create_from_environment()
    except (ValueError, KeyError):
        warn = True
    else:
        warn = bool(env_vars.get_missing_vars())
    if warn:
        print(
            "[bold yellow]WARNING:[/] No .env file found and missing required environment variables. "
            "Searched current or parent directory.",
            file=sys.stderr,
        )
