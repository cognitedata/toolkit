#!/usr/bin/env python
# The Typer parameters get mixed up if we use the __future__ import annotations in the main file.
# ruff: noqa: E402
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, NoReturn, Optional

import typer
from cognite.client.config import global_config

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

from cognite.client.data_classes.data_modeling import NodeId
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.apps import AuthApp, CoreApp, DumpApp, LandingApp, ModulesApp, RepoApp, RunApp
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import (
    CollectCommand,
    DescribeCommand,
    FeatureFlagCommand,
    PullCommand,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
)
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.loaders import (
    NodeLoader,
    TransformationLoader,
)
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    sentry_exception_filter,
)
from cognite_toolkit._version import __version__ as current_version

if "pytest" not in sys.modules and os.environ.get("SENTRY_ENABLED", "true").lower() == "true":
    import sentry_sdk

    sentry_sdk.init(
        dsn="https://ea8b03f98a675ce080056f1583ed9ce7@o124058.ingest.sentry.io/4506429021093888",
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
describe_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
pull_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
feature_flag_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
user_app = typer.Typer(**default_typer_kws, hidden=True)  # type: ignore [arg-type]
landing_app = LandingApp(**default_typer_kws)  # type: ignore [arg-type]

_app.add_typer(AuthApp(**default_typer_kws), name="auth")
_app.add_typer(describe_app, name="describe")
_app.add_typer(RunApp(**default_typer_kws), name="run")
_app.add_typer(RepoApp(**default_typer_kws), name="repo")
_app.add_typer(pull_app, name="pull")
_app.add_typer(DumpApp(**default_typer_kws), name="dump")
_app.add_typer(feature_flag_app, name="features")
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
        print(f"  [bold red]ERROR ([/][red]{type(err).__name__}[/][bold red]):[/] {err}")
        raise SystemExit(1)

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


@describe_app.callback(invoke_without_command=True)
def describe_main(ctx: typer.Context) -> None:
    """Commands to describe and document configurations and CDF project state, use --project (ENV_VAR: CDF_PROJECT) to specify project to use."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf describe --help[/] for more information.")
    return None


@describe_app.command("datamodel")
def describe_datamodel_cmd(
    ctx: typer.Context,
    space: Annotated[
        str,
        typer.Option(
            "--space",
            "-s",
            prompt=True,
            help="Space where the data model to describe is located.",
        ),
    ],
    data_model: Annotated[
        Optional[str],
        typer.Option(
            "--datamodel",
            "-d",
            prompt=False,
            help="Data model to describe. If not specified, the first data model found in the space will be described.",
        ),
    ] = None,
) -> None:
    """This command will describe the characteristics of a data model given the space
    name and datamodel name."""
    cmd = DescribeCommand()
    cmd.run(lambda: cmd.execute(CDFToolConfig.from_context(ctx), space, data_model))


@pull_app.callback(invoke_without_command=True)
def pull_main(ctx: typer.Context) -> None:
    """Commands to download resource configurations from CDF into the module directory."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf pull --help[/] for more information.")


@pull_app.command("transformation")
def pull_transformation_cmd(
    ctx: typer.Context,
    external_id: Annotated[
        str,
        typer.Option(
            "--external-id",
            "-e",
            prompt=True,
            help="External id of the transformation to pull.",
        ),
    ],
    organization_dir: Annotated[
        Path,
        typer.Option(
            "--organization-dir",
            "-o",
            help="Where to find the module templates to build from",
        ),
    ] = CDF_TOML.cdf.default_organization_dir,
    env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="Environment to use.",
        ),
    ] = CDF_TOML.cdf.default_env,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    """This command will pull the specified transformation and update its YAML file in the module folder"""
    cmd = PullCommand()
    cmd.run(
        lambda: cmd.execute(
            organization_dir,
            external_id,
            env,
            dry_run,
            verbose,
            CDFToolConfig.from_context(ctx),
            TransformationLoader,
        )
    )


@pull_app.command("node")
def pull_node_cmd(
    ctx: typer.Context,
    space: Annotated[
        str,
        typer.Option(
            "--space",
            "-s",
            prompt=True,
            help="Space where the node to pull can be found.",
        ),
    ],
    external_id: Annotated[
        str,
        typer.Option(
            "--external-id",
            "-e",
            prompt=True,
            help="External id of the node to pull.",
        ),
    ],
    organization_dir: Annotated[
        Path,
        typer.Option(
            "--organization-dir",
            "-o",
            help="Where to find the module templates to build from",
        ),
    ] = CDF_TOML.cdf.default_organization_dir,
    env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="Environment to use.",
        ),
    ] = CDF_TOML.cdf.default_env,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    """This command will pull the specified node and update its YAML file in the module folder."""

    cmd = PullCommand()
    cmd.run(
        lambda: cmd.execute(
            organization_dir,
            NodeId(space, external_id),
            env,
            dry_run,
            verbose,
            CDFToolConfig.from_context(ctx),
            NodeLoader,
        )
    )


@feature_flag_app.callback(invoke_without_command=True)
def feature_flag_main(ctx: typer.Context) -> None:
    """Commands to enable and disable feature flags for the toolkit."""
    if ctx.invoked_subcommand is None:
        print(
            Panel(
                "[yellow]Warning: enabling feature flags may have undesired side effects."
                "\nDo not enable a flag unless you are familiar with what it does.[/]"
            )
        )
        print("Use [bold yellow]cdf features list[/] available feature flags")
        print(
            f"Use [bold yellow]the section cdf.feature_flags in {CDFToml.file_name!r}[/] to enable or disable feature flags."
        )
    return None


@feature_flag_app.command("list")
def feature_flag_list() -> None:
    """List all available feature flags."""

    cmd = FeatureFlagCommand()
    cmd.run(cmd.list)


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
