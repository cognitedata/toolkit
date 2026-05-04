#!/usr/bin/env python
# The Typer parameters get mixed up if we use the __future__ import annotations in the main file.
# ruff: noqa: E402
import re
import sys
import traceback
from pathlib import Path
from typing import NoReturn

import typer
from cognite.client.config import global_config
from rich.markup import escape
from rich.panel import Panel

from cognite_toolkit._cdf_tk.hints import Hint

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

from rich import print

from cognite_toolkit._cdf_tk.apps import (
    AuthApp,
    CoreApp,
    DataApp,
    DevApp,
    DumpApp,
    ImportApp,
    LandingApp,
    MigrateApp,
    ModulesApp,
    ProfileApp,
    RepoApp,
    RunApp,
)
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import (
    AboutCommand,
)
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, URL, USE_SENTRY
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.plugins import Plugins
from cognite_toolkit._cdf_tk.ui import apply_questionary_toolkit_defaults
from cognite_toolkit._cdf_tk.utils import (
    sentry_exception_filter,
)
from cognite_toolkit._version import __version__ as current_version

apply_questionary_toolkit_defaults()

if USE_SENTRY:
    import sentry_sdk

    sentry_sdk.init(
        dsn="https://20552f92b525fe551e9adc939024d526@o4508040730968064.ingest.de.sentry.io/4508160801374288",
        release=current_version,
        before_send=sentry_exception_filter,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        traces_sample_rate=1.0,
    )


CDF_TOML = CDFToml.load(Path.cwd())

default_typer_kws = dict(
    pretty_exceptions_short=False,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)
try:
    typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
except AttributeError as e:
    # From Typer version 0.11 -> 0.12, breaks if you have an existing installation.
    raise ToolkitError(
        "'cognite-toolkit' uses a dependency named 'typer'. From 'typer' version 0.11 -> 0.12 there was a "
        "breaking change if you have an existing installation of 'typer'. The workaround is to uninstall "
        "'typer-slim', and then, reinstall 'typer':\n"
        "pip uninstall typer-slim\n"
        "pip install typer\n\n"
        f"This was triggered by the error: {e!r}"
    )

_app = CoreApp(**default_typer_kws)

landing_app = LandingApp(**default_typer_kws)

_app.add_typer(AuthApp(**default_typer_kws), name="auth")
_app.add_typer(RepoApp(**default_typer_kws), name="repo")


if Plugins.run.value.is_enabled():
    _app.add_typer(RunApp(**default_typer_kws), name="run")

if Plugins.dump.value.is_enabled():
    _app.add_typer(DumpApp(**default_typer_kws), name="dump")


if Plugins.dev.value.is_enabled():
    _app.add_typer(DevApp(**default_typer_kws), name="dev")

if Flags.PROFILE.is_enabled():
    _app.add_typer(ProfileApp(**default_typer_kws), name="profile")

if Flags.MIGRATE.is_enabled():
    _app.add_typer(MigrateApp(**default_typer_kws), name="migrate")

if Flags.IMPORT_CMD.is_enabled():
    _app.add_typer(ImportApp(**default_typer_kws), name="import")

if Plugins.data.value.is_enabled():
    _app.add_typer(DataApp(**default_typer_kws), name="data")


_app.add_typer(ModulesApp(**default_typer_kws), name="modules")
_app.command("init")(landing_app.main_init)


@_app.command("about")
def about() -> None:
    """Display information about the Toolkit installation and configuration."""
    cmd = AboutCommand()
    cmd.run(lambda: cmd.execute(Path.cwd()))


def _get_subcommand_map() -> dict[str, list[str]]:
    """Build a map from subcommand names to their full command paths.

    Returns a dict where keys are subcommand names and values are lists of
    full command paths (e.g., {"download": ["cdf data download"]}).
    """
    subcommand_map: dict[str, list[str]] = {}

    def _add_commands_from_typer(typer_app: typer.Typer, prefix: str) -> None:
        # Get registered commands
        for command in typer_app.registered_commands:
            cmd_name = command.name or (command.callback.__name__ if command.callback else None)
            if cmd_name:
                full_path = f"{prefix} {cmd_name}"
                subcommand_map.setdefault(cmd_name, []).append(full_path)

        # Get registered sub-typers (groups)
        for group in typer_app.registered_groups:
            group_name = group.name
            if group_name and group.typer_instance:
                # Add the group name itself as a command path
                full_path = f"{prefix} {group_name}"
                subcommand_map.setdefault(group_name, []).append(full_path)
                # Recursively add commands from the sub-typer
                _add_commands_from_typer(group.typer_instance, full_path)

    _add_commands_from_typer(_app, "cdf")
    return subcommand_map


def _suggest_command(unknown_cmd: str) -> str | None:
    """Check if the unknown command exists as a subcommand and return a suggestion."""
    subcommand_map = _get_subcommand_map()
    if unknown_cmd in subcommand_map:
        paths = subcommand_map[unknown_cmd]
        if len(paths) == 1:
            return f"Did you mean [bold]{paths[0]}[/bold]?"
        return "Did you mean one of: " + ", ".join(f"[bold]{p}[/bold]" for p in paths) + "?"
    return None


def app() -> NoReturn:
    # --- Main entry point ---
    # Strip --traceback from sys.argv before Typer processes it (hidden debug flag)
    show_traceback = "--traceback" in sys.argv
    if show_traceback:
        sys.argv.remove("--traceback")

    # Users run 'app()' directly, but that doesn't allow us to control excepton handling:
    try:
        _app()
    except ToolkitError as err:
        if show_traceback:
            print(Panel(traceback.format_exc(), title="Traceback", expand=False))

        print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
        raise SystemExit(1)
    except SystemExit:
        if result := re.search(r"click.exceptions.UsageError: No such command '(\w+)'.", traceback.format_exc()):
            cmd = result.group(1)
            if cmd in Plugins.list():
                plugin = r"[plugins]"
                print(
                    f"{HINT_LEAD_TEXT} The plugin [bold]{cmd}[/bold] is not enabled."
                    f"\nEnable it in the [bold]cdf.toml[/bold] file by setting '{cmd} = true' in the "
                    f"[bold]{escape(plugin)}[/bold] section."
                    f"\nDocs to learn more: {Hint.link(URL.plugins, URL.plugins)}"
                )
            elif suggestion := _suggest_command(cmd):
                print(f"{HINT_LEAD_TEXT} {suggestion}")
        raise

    raise SystemExit(0)


if __name__ == "__main__":
    app()
