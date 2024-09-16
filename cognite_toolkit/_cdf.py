#!/usr/bin/env python
# The Typer parameters get mixed up if we use the __future__ import annotations in the main file.
# ruff: noqa: E402
import contextlib
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, NoReturn, Optional, Union

import typer
from cognite.client.config import global_config

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

from cognite.client.data_classes.data_modeling import DataModelId, NodeId
from dotenv import load_dotenv
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.apps import AuthApp, LandingApp, ModulesApp, RepoApp, RunApp
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import (
    AuthCommand,
    BuildCommand,
    CleanCommand,
    CollectCommand,
    DeployCommand,
    DescribeCommand,
    DumpCommand,
    FeatureFlagCommand,
    PullCommand,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
    ToolkitFileNotFoundError,
    ToolkitInvalidSettingsError,
    ToolkitValidationError,
)
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    NodeLoader,
    TransformationLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitDeprecationWarning
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

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

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

_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
auth_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
describe_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
pull_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
dump_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
feature_flag_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
user_app = typer.Typer(**default_typer_kws, hidden=True)  # type: ignore [arg-type]
modules_app = ModulesApp(**default_typer_kws)  # type: ignore [arg-type]
landing_app = LandingApp(**default_typer_kws)  # type: ignore [arg-type]

_app.add_typer(AuthApp(**default_typer_kws), name="auth")
_app.add_typer(describe_app, name="describe")
_app.add_typer(RunApp(**default_typer_kws), name="run")
_app.add_typer(RepoApp(**default_typer_kws), name="repo")
_app.add_typer(pull_app, name="pull")
_app.add_typer(dump_app, name="dump")
_app.add_typer(feature_flag_app, name="features")
_app.add_typer(modules_app, name="modules")
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


_AVAILABLE_DATA_TYPES: tuple[str, ...] = tuple(LOADER_BY_FOLDER_NAME)


# Common parameters handled in common callback
@dataclass
class Common:
    override_env: bool
    verbose: bool
    cluster: Union[str, None]
    project: Union[str, None]
    mockToolGlobals: Union[CDFToolConfig, None]


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"CDF-Toolkit version: {current_version}.")
        raise typer.Exit()


@_app.callback(invoke_without_command=True)
def common(
    ctx: typer.Context,
    verbose: Annotated[
        bool,
        typer.Option(
            help="Turn on to get more verbose output when running the commands",
        ),
    ] = False,
    override_env: Annotated[
        bool,
        typer.Option(
            help="Load the .env file in this or the parent directory, but also override currently set environment variables",
        ),
    ] = False,
    env_path: Annotated[
        Optional[str],
        typer.Option(
            help="Path to .env file to load. Defaults to .env in current or parent directory.",
        ),
    ] = None,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="See which version of the tooklit and the templates are installed.",
            callback=_version_callback,
        ),
    ] = False,
) -> None:
    """
    Docs: https://docs.cognite.com/cdf/deploy/cdf_toolkit/\n
    Template reference documentation: https://developer.cognite.com/sdks/toolkit/references/configs
    """
    if ctx.invoked_subcommand is None:
        print(
            Panel(
                "\n".join(
                    [
                        "The Cognite Data Fusion Toolkit supports configuration of CDF projects from the command line or in CI/CD pipelines.",
                        "",
                        "[bold]Setup:[/]",
                        "1. Run [underline]cdf repo init[/] [italic]<directory name>[/] to set up a work directory.",
                        "2. Run [underline]cdf modules init[/] [italic]<directory name>[/] to initialise configuration modules.",
                        "",
                        "[bold]Configuration steps:[/]",
                        "3. Run [underline]cdf build[/] [italic]<directory name>[/] to verify the configuration for your project. Repeat for as many times as needed.",
                        "   Tip:[underline]cdf modules list[/] [italic]<directory name>[/] gives an overview of all your modules and their status.",
                        "",
                        "[bold]Deployment steps:[/]",
                        "4. Commit the [italic]<directory name>[/] to version control",
                        "5. Run [underline]cdf auth verify --interactive[/] to check that you have access to the relevant CDF project. ",
                        "    or [underline]cdf auth verify[/] if you have a .env file",
                        "6. Run [underline]cdf deploy --dry-run[/] to simulate the deployment of the configuration to the CDF project. Review the report provided.",
                        "7. Run [underline]cdf deploy[/] to deploy the configuration to the CDF project.",
                    ]
                ),
                title="Getting started",
                style="green",
                padding=(1, 2),
            )
        )
        return
    if override_env:
        print("  [bold yellow]WARNING:[/] Overriding environment variables with values from .env file...")

    if env_path is not None:
        if not (dotenv_file := Path(env_path)).is_file():
            raise ToolkitFileNotFoundError(env_path)

    else:
        if not (dotenv_file := Path.cwd() / ".env").is_file():
            if not (dotenv_file := Path.cwd().parent / ".env").is_file():
                print("[bold yellow]WARNING:[/] No .env file found in current or parent directory.")

    if dotenv_file.is_file():
        if verbose:
            try:
                path_str = dotenv_file.relative_to(Path.cwd())
            except ValueError:
                path_str = dotenv_file.absolute()
            print(f"Loading .env file: {path_str!s}.")
        has_loaded = load_dotenv(dotenv_file, override=override_env)
        if not has_loaded:
            print("  [bold yellow]WARNING:[/] No environment variables found in .env file.")

    ctx.obj = Common(
        verbose=verbose,
        override_env=override_env,
        cluster=None,
        project=None,
        mockToolGlobals=None,
    )


@_app.command("build")
def build(
    ctx: typer.Context,
    organization_dir: Annotated[
        Path,
        typer.Option(
            "--organization-dir",
            "-o",
            help="Where to find the module templates to build from",
        ),
    ] = CDF_TOML.cdf.default_organization_dir,
    build_dir: Annotated[
        str,
        typer.Option(
            "--build-dir",
            "-b",
            help="Where to save the built module files",
        ),
    ] = "./build",
    build_env_name: Annotated[
        str,
        typer.Option(
            "--env",
            "-e",
            help="The name of the environment to build",
        ),
    ] = CDF_TOML.cdf.default_env,
    no_clean: Annotated[
        bool,
        typer.Option(
            "--no-clean",
            "-c",
            help="Whether not to delete the build directory before building the configurations",
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
    """Build configuration files from the module templates to a local build directory."""
    ToolGlobals: Union[CDFToolConfig, None] = None
    with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
        # Remove the Error message from failing to load the config
        # This is verified in check_auth
        ToolGlobals = CDFToolConfig()

    cmd = BuildCommand()
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk build --verbose").get_message())
    cmd.run(
        lambda: cmd.execute(
            verbose or ctx.obj.verbose,
            Path(organization_dir),
            Path(build_dir),
            build_env_name,
            no_clean,
            ToolGlobals,
        )
    )


@_app.command("deploy")
def deploy(
    ctx: typer.Context,
    build_dir: Annotated[
        str,
        typer.Argument(
            help="Where to find the module templates to deploy from. Defaults to current directory.",
            allow_dash=True,
        ),
    ] = "./build",
    build_env_name: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to use for deployment. This is optional and "
            "if passed it is used to verify against the build environment",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Whether to use interactive mode when deciding which modules to deploy.",
        ),
    ] = False,
    drop: Annotated[
        bool,
        typer.Option(
            "--drop",
            "-d",
            help="Whether to drop existing configurations, drop per resource if present.",
        ),
    ] = False,
    drop_data: Annotated[
        bool,
        typer.Option(
            "--drop-data",
            help="Whether to drop existing data in data model containers and spaces.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    include: Annotated[
        Optional[list[str]],
        typer.Option(
            "--include",
            help=f"Specify which resources to deploy, available options: {_AVAILABLE_DATA_TYPES}.",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    cmd = DeployCommand(print_warning=True)
    include = _process_include(include, interactive)
    ToolGlobals = CDFToolConfig.from_context(ctx)
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk deploy --verbose").get_message())
    cmd.run(
        lambda: cmd.execute(
            ToolGlobals,
            build_dir,
            build_env_name,
            dry_run,
            drop,
            drop_data,
            include,
            verbose or ctx.obj.verbose,
        )
    )


@_app.command("clean")
def clean(
    ctx: typer.Context,
    build_dir: Annotated[
        str,
        typer.Argument(
            help="Where to find the module templates to clean from. Defaults to ./build directory.",
            allow_dash=True,
        ),
    ] = "./build",
    build_env_name: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to use for cleaning. This is optional and "
            "if passed it is used to verify against the build environment",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Whether to use interactive mode when deciding which resource types to clean.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present",
        ),
    ] = False,
    include: Annotated[
        Optional[list[str]],
        typer.Option(
            "--include",
            help=f"Specify which resource types to deploy, supported types: {_AVAILABLE_DATA_TYPES}",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    """Clean up a CDF environment as set in environments.yaml restricted to the entities in the configuration files in the build directory."""
    # Override cluster and project from the options/env variables
    cmd = CleanCommand(print_warning=True)
    include = _process_include(include, interactive)
    ToolGlobals = CDFToolConfig.from_context(ctx)
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk clean --verbose").get_message())
    cmd.run(
        lambda: cmd.execute(
            ToolGlobals,
            build_dir,
            build_env_name,
            dry_run,
            include,
            verbose or ctx.obj.verbose,
        )
    )


@_app.command("collect", hidden=True)
def collect(
    action: str = typer.Argument(
        help="Whether to explicitly opt-in or opt-out of usage data collection. [opt-in, opt-out]"
    ),
) -> None:
    """Collect usage information for the toolkit."""
    cmd = CollectCommand()
    cmd.run(lambda: cmd.execute(action))  # type: ignore [arg-type]


@auth_app.callback(invoke_without_command=True)
def auth_main(ctx: typer.Context) -> None:
    """Test, validate, and configure authentication and authorization for CDF projects."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf auth --help[/] for more information.")
    return None


@auth_app.command("verify")
def auth_verify(
    ctx: typer.Context,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Will run the verification in interactive mode, prompting for input. Used to bootstrap a new project."
            "If this mode is selected the --update-group and --create-group options will be ignored.",
        ),
    ] = False,
    group_file: Annotated[
        Optional[str],
        typer.Option(
            "--group-file",
            "-f",
            help="Path to group yaml configuration file to use for group verification. "
            "Defaults to admin.readwrite.group.yaml from the cdf_auth_readwrite_all common module.",
        ),
    ] = None,
    update_group: Annotated[
        int,
        typer.Option(
            "--update-group",
            "-u",
            help="If --interactive is not set. Used to update an existing group with the configurations."
            "Set to the group id or 1 to update the default write-all group.",
        ),
    ] = 0,
    create_group: Annotated[
        Optional[str],
        typer.Option(
            "--create-group",
            "-c",
            help="If --interactive is not set. Used to create a new group with the configurations."
            "Set to the source id of the new group.",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    """When you have the necessary information about your identity provider configuration,
    you can use this command to configure the tool and verify that the token has the correct access rights to the project.
    It can also create a group with the correct access rights, defaulting to write-all group
    meant for an admin/CICD pipeline.

    As a minimum, you need the CDF project name, the CDF cluster, an identity provider token URL, and a service account client ID
    and client secret (or an OAuth2 token set in CDF_TOKEN environment variable).

    Needed capabilities for bootstrapping:
    "projectsAcl": ["LIST", "READ"],
    "groupsAcl": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"]

    The default bootstrap group configuration is admin.readwrite.group.yaml from the cdf_auth_readwrite_all common module.
    """
    cmd = AuthCommand()
    with contextlib.redirect_stdout(None):
        # Remove the Error message from failing to load the config
        # This is verified in check_auth
        ToolGlobals = CDFToolConfig(cluster=ctx.obj.cluster, project=ctx.obj.project, skip_initialization=True)
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk auth verify --verbose").get_message())
    cmd.run(
        lambda: cmd.execute(
            ToolGlobals,
            dry_run,
            interactive,
            group_file,
            update_group,
            create_group,
            verbose or ctx.obj.verbose,
        )
    )


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
        str,
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
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk pull transformation --verbose").get_message())
    cmd = PullCommand()
    cmd.run(
        lambda: cmd.execute(
            organization_dir,
            external_id,
            env,
            dry_run,
            verbose or ctx.obj.verbose,
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
        str,
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
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk pull node --verbose").get_message())

    cmd = PullCommand()
    cmd.run(
        lambda: cmd.execute(
            organization_dir,
            NodeId(space, external_id),
            env,
            dry_run,
            verbose or ctx.obj.verbose,
            CDFToolConfig.from_context(ctx),
            NodeLoader,
        )
    )


@dump_app.callback(invoke_without_command=True)
def dump_main(ctx: typer.Context) -> None:
    """Commands to dump resource configurations from CDF into a temporary directory."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf dump --help[/] for more information.")
    return None


@dump_app.command("datamodel")
def dump_datamodel_cmd(
    ctx: typer.Context,
    space: Annotated[
        str,
        typer.Option(
            "--space",
            "-s",
            prompt=True,
            help="Space where the datamodel to pull can be found.",
        ),
    ],
    external_id: Annotated[
        str,
        typer.Option(
            "--external-id",
            "-e",
            prompt=True,
            help="External id of the datamodel to pull.",
        ),
    ],
    version: Annotated[
        Optional[str],
        typer.Option(
            "--version",
            "-v",
            help="Version of the datamodel to pull.",
        ),
    ] = None,
    clean: Annotated[
        bool,
        typer.Option(
            "--clean",
            "-c",
            help="Delete the output directory before pulling the datamodel.",
        ),
    ] = False,
    output_dir: Annotated[
        str,
        typer.Argument(
            help="Where to dump the datamodel YAML files.",
            allow_dash=True,
        ),
    ] = "tmp",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Turn on to get more verbose output when running the command",
        ),
    ] = False,
) -> None:
    """This command will dump the selected data model as yaml to the folder specified, defaults to /tmp."""
    cmd = DumpCommand()
    if ctx.obj.verbose:
        print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk dump datamodel --verbose").get_message())
    cmd.run(
        lambda: cmd.execute(
            CDFToolConfig.from_context(ctx),
            DataModelId(space, external_id, version),
            Path(output_dir),
            clean,
            verbose or ctx.obj.verbose,
        )
    )


if FeatureFlag.is_enabled(Flags.ASSETS):
    from cognite_toolkit._cdf_tk.prototypes.commands import DumpAssetsCommand

    @dump_app.command("asset")
    def dump_asset_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            Optional[list[str]],
            typer.Option(
                "--hierarchy",
                "-h",
                help="Hierarchy to dump.",
            ),
        ] = None,
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump.",
            ),
        ] = None,
        interactive: Annotated[
            bool,
            typer.Option(
                "--interactive",
                "-i",
                help="Will prompt you to select which assets hierarchies to dump.",
            ),
        ] = False,
        output_dir: Annotated[
            Path,
            typer.Argument(
                help="Where to dump the asset YAML files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the assets in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        clean_: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before pulling the assets.",
            ),
        ] = False,
        limit: Annotated[
            Optional[int],
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of assets to dump.",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will dump the selected assets as yaml to the folder specified, defaults to /tmp."""
        cmd = DumpAssetsCommand()
        if ctx.obj.verbose:
            print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk dump asset --verbose").get_message())
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                hierarchy,
                data_set,
                interactive,
                output_dir,
                clean_,
                limit,
                format_,  # type: ignore [arg-type]
                verbose or ctx.obj.verbose,
            )
        )


if FeatureFlag.is_enabled(Flags.TIMESERIES_DUMP):
    from cognite_toolkit._cdf_tk.prototypes.commands import DumpTimeSeriesCommand

    @dump_app.command("timeseries")
    def dump_timeseries_cmd(
        ctx: typer.Context,
        time_series_list: Annotated[
            Optional[list[str]],
            typer.Option(
                "--timeseries",
                "-t",
                help="Timeseries to dump.",
            ),
        ] = None,
        data_set: Annotated[
            Optional[list[str]],
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump.",
            ),
        ] = None,
        interactive: Annotated[
            bool,
            typer.Option(
                "--interactive",
                "-i",
                help="Will prompt you to select which timeseries to dump.",
            ),
        ] = False,
        output_dir: Annotated[
            Path,
            typer.Argument(
                help="Where to dump the timeseries YAML files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the timeseries in. Supported formats: yaml, csv, and parquet.",
            ),
        ] = "csv",
        clean_: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before pulling the timeseries.",
            ),
        ] = False,
        limit: Annotated[
            Optional[int],
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of timeseries to dump.",
            ),
        ] = None,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will dump the selected timeseries as yaml to the folder specified, defaults to /tmp."""
        cmd = DumpTimeSeriesCommand()
        if ctx.obj.verbose:
            print(ToolkitDeprecationWarning("cdf-tk --verbose", "cdf-tk dump timeseries --verbose").get_message())
        cmd.run(
            lambda: cmd.execute(
                CDFToolConfig.from_context(ctx),
                data_set,
                interactive,
                output_dir,
                clean_,
                limit,
                format_,  # type: ignore [arg-type]
                verbose or ctx.obj.verbose,
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


def _process_include(include: Optional[list[str]], interactive: bool) -> list[str]:
    if include and (invalid_types := set(include).difference(_AVAILABLE_DATA_TYPES)):
        raise ToolkitValidationError(
            f"Invalid resource types specified: {invalid_types}, available types: {_AVAILABLE_DATA_TYPES}"
        )
    include = include or list(_AVAILABLE_DATA_TYPES)
    if interactive:
        include = _select_data_types(include)
    return include


def _select_data_types(include: Sequence[str]) -> list[str]:
    mapping: dict[int, str] = {}
    for i, datatype in enumerate(include):
        print(f"[bold]{i})[/] {datatype}")
        mapping[i] = datatype
    print("\na) All")
    print("q) Quit")
    answer = input("Select resource types to include: ")
    if answer.casefold() == "a":
        return list(include)
    elif answer.casefold() == "q":
        raise SystemExit(0)
    else:
        try:
            return [mapping[int(answer)]]
        except ValueError:
            raise ToolkitInvalidSettingsError(f"Invalid selection: {answer}")


if __name__ == "__main__":
    app()
