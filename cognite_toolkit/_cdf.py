#!/usr/bin/env python
# The Typer parameters get mixed up if we use the __future__ import annotations in the main file.
# ruff: noqa: E402
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn

import typer
from cognite.client.config import global_config
from rich.panel import Panel

from cognite_toolkit._cdf_tk.hints import Hint

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

from rich import print

from cognite_toolkit._cdf_tk.apps import (
    AuthApp,
    CoreApp,
    DumpApp,
    LandingApp,
    ModulesApp,
    PullApp,
    PurgeApp,
    RepoApp,
    RunApp,
)
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import (
    CollectCommand,
)
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, URL, USE_SENTRY
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
)
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.plugins import Plugins
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.utils import (
    sentry_exception_filter,
)
from cognite_toolkit._version import __version__ as current_version

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

user_app = typer.Typer(**default_typer_kws, hidden=True)  # type: ignore [arg-type]
landing_app = LandingApp(**default_typer_kws)  # type: ignore [arg-type]

_app.add_typer(AuthApp(**default_typer_kws), name="auth")
if Plugins.run.value.is_enabled():
    _app.add_typer(RunApp(**default_typer_kws), name="run")
_app.add_typer(RepoApp(**default_typer_kws), name="repo")

if Plugins.pull.value.is_enabled():
    _app.add_typer(PullApp(**default_typer_kws), name="pull")

if Plugins.dump.value.is_enabled():
    _app.add_typer(DumpApp(**default_typer_kws), name="dump")

if Flags.PURGE.is_enabled():
    _app.add_typer(PurgeApp(**default_typer_kws), name="purge")


_app.add_typer(ModulesApp(**default_typer_kws), name="modules")
_app.command("init")(landing_app.main_init)


def app() -> NoReturn:
    # --- Main entry point ---
    # Users run 'app()' directly, but that doesn't allow us to control excepton handling:
    try:
        if FeatureFlag.is_enabled(Flags.IMPORT_CMD):
            from cognite_toolkit._cdf_tk.prototypes.import_app import import_app

            _app.add_typer(import_app, name="import")

        # Secret plugin, this will be removed without warning
        # This should not be documented, or raise any error or warnings,
        # just fail silently if the plugin is not found or not correctly setup.
        dev_py = Path.cwd() / "dev.py"
        if dev_py.exists():
            from importlib.util import module_from_spec, spec_from_file_location

            spec = spec_from_file_location("dev", dev_py)
            if spec and spec.loader:
                dev_module = module_from_spec(spec)
                spec.loader.exec_module(dev_module)
                if "CDF_TK_PLUGIN" in dev_module.__dict__:
                    command_by_name = {cmd.name: cmd for cmd in _app.registered_commands}
                    group_by_name = {group.name: group for group in _app.registered_groups}
                    for name, type_app in dev_module.__dict__["CDF_TK_PLUGIN"].items():
                        if not isinstance(type_app, typer.Typer):
                            continue
                        if name in command_by_name:
                            # We are not allowed to replace an existing command.
                            continue
                        elif name in group_by_name:
                            group = group_by_name[name]
                            if group.typer_instance is None:
                                continue
                            existing_command_names = {cmd.name for cmd in group.typer_instance.registered_commands}
                            for new_command in type_app.registered_commands:
                                if new_command.name in existing_command_names:
                                    # We are not allowed to replace an existing command.
                                    continue
                                group.typer_instance.command(new_command.name)(new_command.callback)  # type: ignore [type-var]
                        else:
                            if type_app.registered_groups:
                                _app.add_typer(type_app, name=name)
                            else:
                                for app_cmd in type_app.registered_commands:
                                    if app_cmd.name not in command_by_name:
                                        _app.command(app_cmd.name)(app_cmd.callback)  # type: ignore [type-var]

        _app()
    except ToolkitError as err:
        if "--verbose" in sys.argv:
            print(Panel(traceback.format_exc(), title="Traceback", expand=False))

        print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
        raise SystemExit(1)
    except SystemExit:
        if result := re.search(r"click.exceptions.UsageError: No such command '(\w+)'.", traceback.format_exc()):
            cmd = result.group(1)
            if cmd in Plugins.list():
                print(
                    f"{HINT_LEAD_TEXT} The plugin [bold]{cmd}[/bold] is not enabled."
                    f"\nEnable it in the [bold]cdf.toml[/bold] file by setting '{cmd} = true' in the \[plugins] section."
                    f"\nDocs to lean more: {Hint._link(URL.plugins, URL.plugins)}"
                )
        raise

    raise SystemExit(0)


@_app.command("collect", hidden=True)
def collect(
    action: str = typer.Argument(
        help="Whether to explicitly opt-in or opt-out of usage data collection. [opt-in, opt-out]"
    ),
) -> None:
    """Collect usage information for the toolkit."""
    cmd = CollectCommand()
    cmd.run(lambda: cmd.execute(action))  # type: ignore [arg-type]


@user_app.callback(invoke_without_command=True)
def user_main(ctx: typer.Context) -> None:
    """Commands to give information about the toolkit."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf user --help[/] to see available commands.")
    return None


@user_app.command("info")
def user_info() -> None:
    """Print information about user"""
    tracker = Tracker()
    print(f"ID={tracker.get_distinct_id()!r}\nnow={datetime.now(timezone.utc).isoformat(timespec='seconds')!r}")


if __name__ == "__main__":
    app()
