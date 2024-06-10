#!/usr/bin/env python
# The Typer parameters get mixed up if we use the __future__ import annotations in the main file.

import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, NoReturn, Optional, Union

import typer
from cognite.client.data_classes.data_modeling import DataModelId, NodeId
from dotenv import load_dotenv
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands import (
    AuthCommand,
    BuildCommand,
    CleanCommand,
    DeployCommand,
    DescribeCommand,
    DumpCommand,
    PullCommand,
    RunFunctionCommand,
    RunTransformationCommand,
)
from cognite_toolkit._cdf_tk.data_classes import (
    ProjectDirectoryInit,
    ProjectDirectoryUpgrade,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
    ToolkitFileNotFoundError,
    ToolkitInvalidSettingsError,
    ToolkitValidationError,
)
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    NodeLoader,
    TransformationLoader,
)
from cognite_toolkit._cdf_tk.prototypes import featureflag
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

default_typer_kws = dict(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
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
run_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
pull_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
dump_app = typer.Typer(**default_typer_kws)  # type: ignore [arg-type]
_app.add_typer(auth_app, name="auth")
_app.add_typer(describe_app, name="describe")
_app.add_typer(run_app, name="run")
_app.add_typer(pull_app, name="pull")
_app.add_typer(dump_app, name="dump")


def app() -> NoReturn:
    # --- Main entry point ---
    # Users run 'app()' directly, but that doesn't allow us to control excepton handling:
    try:
        if featureflag.enabled("FF_INTERACTIVE_INIT"):
            from cognite_toolkit._cdf_tk.prototypes.interactive_init import InteractiveInit

            _app.command("init")(InteractiveInit().interactive)
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
    cluster: Annotated[
        Optional[str],
        typer.Option(
            envvar="CDF_CLUSTER",
            help="The Cognite Data Fusion cluster to use. Can also be set with the CDF_CLUSTER environment variable.",
        ),
    ] = None,
    project: Annotated[
        Optional[str],
        typer.Option(
            envvar="CDF_PROJECT",
            help="The Cognite Data Fusion project to use. Can also be set with the CDF_PROJECT environment variable.",
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
    """The cdf-tk tool is used to build and deploy Cognite Data Fusion project configurations from the command line or through CI/CD pipelines.

    Each of the main commands has a separate help, e.g. `cdf-tk build --help` or `cdf-tk deploy --help`.

    You can find the documation at https://developer.cognite.com/sdks/toolkit/
    and the template reference documentation at https://developer.cognite.com/sdks/toolkit/references/configs
    """
    if ctx.invoked_subcommand is None:
        print(
            "[bold]A tool to manage and deploy Cognite Data Fusion project configurations from the command line or through CI/CD pipelines.[/]"
        )
        print("[bold yellow]Usage:[/] cdf-tk [OPTIONS] COMMAND [ARGS]...")
        print("       Use --help for more information.")
        return
    if override_env:
        print("  [bold yellow]WARNING:[/] Overriding environment variables with values from .env file...")
        if cluster is not None or project is not None:
            print("            --cluster or --project is set and will override .env file values.")

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
        cluster=cluster,
        project=project,
        mockToolGlobals=None,
    )


@_app.command("build")
def build(
    ctx: typer.Context,
    source_dir: Annotated[
        str,
        typer.Argument(
            help="Where to find the module templates to build from",
            allow_dash=True,
        ),
    ] = "./",
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
    ] = "dev",
    no_clean: Annotated[
        bool,
        typer.Option(
            "--no-clean", "-c", help="Whether not to delete the build directory before building the configurations"
        ),
    ] = False,
) -> None:
    """Build configuration files from the module templates to a local build directory."""
    user_command = f"cdf-tk {' '.join(sys.argv[1:])}"
    cmd = BuildCommand(user_command=user_command)
    cmd.execute(ctx.obj.verbose, Path(source_dir), Path(build_dir), build_env_name, no_clean)


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
        str,
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to build for. Defined in environments.yaml.",
        ),
    ] = "dev",
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
            "-i",
            help=f"Specify which resources to deploy, available options: {_AVAILABLE_DATA_TYPES}.",
        ),
    ] = None,
) -> None:
    cmd = DeployCommand(print_warning=True)
    include = _process_include(include, interactive)
    cmd.execute(ctx, build_dir, build_env_name, dry_run, drop, drop_data, include)


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
        str,
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to use for cleaning.",
        ),
    ] = "dev",
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
            "-i",
            help=f"Specify which resource types to deploy, supported types: {_AVAILABLE_DATA_TYPES}",
        ),
    ] = None,
) -> None:
    """Clean up a CDF environment as set in environments.yaml restricted to the entities in the configuration files in the build directory."""
    # Override cluster and project from the options/env variables
    cmd = CleanCommand(print_warning=True)
    include = _process_include(include, interactive)
    cmd.execute(ctx, build_dir, build_env_name, dry_run, include)


@auth_app.callback(invoke_without_command=True)
def auth_main(ctx: typer.Context) -> None:
    """Test, validate, and configure authentication and authorization for CDF projects."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk auth --help[/] for more information.")
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
            help="Will run the verification in interactive mode, prompting for input. Used to bootstrap a new project.",
        ),
    ] = False,
    group_file: Annotated[
        Optional[str],
        typer.Option(
            "--group-file",
            "-f",
            help="Path to group yaml configuration file to use for group verification. Defaults to admin.readwrite.group.yaml from the cdf_auth_readwrite_all common module.",
        ),
    ] = None,
    update_group: Annotated[
        int,
        typer.Option(
            "--update-group",
            "-u",
            help="Used to update an existing group with the configurations from the configuration file. Set to the group id to update or 1 to update the default write-all group (if the tool is only member of one group).",
        ),
    ] = 0,
    create_group: Annotated[
        Optional[str],
        typer.Option(
            "--create-group",
            "-c",
            help="Used to create a new group with the configurations from the configuration file. Set to the source id that the new group should be configured with.",
        ),
    ] = None,
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
    cmd.execute(ctx, dry_run, interactive, group_file, update_group, create_group)


@_app.command("init" if not featureflag.enabled("FF_INTERACTIVE_INIT") else "_init")
def main_init(
    ctx: typer.Context,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    upgrade: Annotated[
        bool,
        typer.Option(
            "--upgrade",
            "-u",
            help="Will upgrade templates in place without overwriting existing config.yaml and other files.",
        ),
    ] = False,
    git_branch: Annotated[
        Optional[str],
        typer.Option(
            "--git",
            "-g",
            help="Will download the latest templates from the git repository branch specified. Use `main` to get the very latest templates.",
        ),
    ] = None,
    no_backup: Annotated[
        bool,
        typer.Option(
            "--no-backup",
            help="Will skip making a backup before upgrading.",
        ),
    ] = False,
    clean: Annotated[
        bool,
        typer.Option(
            "--clean",
            help="Will delete the new_project directory before starting.",
        ),
    ] = False,
    init_dir: Annotated[
        str,
        typer.Argument(
            help="Directory path to project to initialize or upgrade with templates.",
        ),
    ] = "new_project",
) -> None:
    """Initialize or upgrade a new CDF project with templates."""
    project_dir: Union[ProjectDirectoryUpgrade, ProjectDirectoryInit]
    if upgrade:
        project_dir = ProjectDirectoryUpgrade(Path.cwd() / f"{init_dir}", dry_run)
        if project_dir.cognite_module_version == current_version:
            print("No changes to the toolkit detected.")
            typer.Exit()
    else:
        project_dir = ProjectDirectoryInit(Path.cwd() / f"{init_dir}", dry_run)

    verbose = ctx.obj.verbose

    project_dir.set_source(git_branch)

    project_dir.create_project_directory(clean)

    if isinstance(project_dir, ProjectDirectoryUpgrade):
        project_dir.do_backup(no_backup, verbose)

    project_dir.print_what_to_copy()

    project_dir.copy(verbose)

    project_dir.upsert_config_yamls(clean)

    if not dry_run:
        print(Panel(project_dir.done_message()))

    if isinstance(project_dir, ProjectDirectoryUpgrade):
        project_dir.print_manual_steps()


@describe_app.callback(invoke_without_command=True)
def describe_main(ctx: typer.Context) -> None:
    """Commands to describe and document configurations and CDF project state, use --project (ENV_VAR: CDF_PROJECT) to specify project to use."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk describe --help[/] for more information.")
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
    cmd.execute(CDFToolConfig.from_context(ctx), space, data_model)


@run_app.callback(invoke_without_command=True)
def run_main(ctx: typer.Context) -> None:
    """Commands to execute processes in CDF, use --project (ENV_VAR: CDF_PROJECT) to specify project to use."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk run --help[/] for more information.")


@run_app.command("transformation")
def run_transformation_cmd(
    ctx: typer.Context,
    external_id: Annotated[
        str,
        typer.Option(
            "--external-id",
            "-e",
            prompt=True,
            help="External id of the transformation to run.",
        ),
    ],
) -> None:
    """This command will run the specified transformation using a one-time session."""
    cmd = RunTransformationCommand()
    cmd.run_transformation(CDFToolConfig.from_context(ctx), external_id)


@run_app.command("function")
def run_function_cmd(
    ctx: typer.Context,
    external_id: Annotated[
        str,
        typer.Option(
            "--external-id",
            "-e",
            prompt=True,
            help="External id of the function to run.",
        ),
    ],
    payload: Annotated[
        Optional[str],
        typer.Option(
            "--payload",
            "-p",
            help='Payload to send to the function, remember to escape " with \\.',
        ),
    ] = None,
    follow: Annotated[
        bool,
        typer.Option(
            "--follow",
            "-f",
            help="Use follow to wait for results of function.",
        ),
    ] = False,
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            "-l",
            help="Run the function locally in a virtual environment.",
        ),
    ] = False,
    rebuild_env: Annotated[
        bool,
        typer.Option(
            "--rebuild-env",
            "-r",
            help="Rebuild the virtual environment.",
        ),
    ] = False,
    no_cleanup: Annotated[
        bool,
        typer.Option(
            "--no-cleanup",
            "-n",
            help="Do not delete the temporary build directory.",
        ),
    ] = False,
    source_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to build from",
        ),
    ] = None,
    schedule: Annotated[
        Optional[str],
        typer.Option(
            "--schedule",
            "-s",
            help="Run the function locally with the credentials from the schedule specified with the cron expression.",
        ),
    ] = None,
    build_env_name: Annotated[
        str,
        typer.Option(
            "--env",
            "-e",
            help="Build environment to build for",
        ),
    ] = "dev",
) -> None:
    """This command will run the specified function using a one-time session."""
    cmd = RunFunctionCommand()
    cmd.execute(
        CDFToolConfig.from_context(ctx),
        external_id,
        payload,
        follow,
        local,
        rebuild_env,
        no_cleanup,
        source_dir,
        schedule,
        build_env_name,
        ctx.obj.verbose,
    )


@pull_app.callback(invoke_without_command=True)
def pull_main(ctx: typer.Context) -> None:
    """Commands to download resource configurations from CDF into the module directory."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk pull --help[/] for more information.")


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
    source_dir: Annotated[
        str,
        typer.Argument(
            help="Where to find the destination module templates (project directory).",
            allow_dash=True,
        ),
    ] = "./",
    env: Annotated[
        str,
        typer.Option(
            "--env",
            "-e",
            help="Environment to use.",
        ),
    ] = "dev",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
) -> None:
    """This command will pull the specified transformation and update its YAML file in the module folder"""
    PullCommand().execute(
        source_dir, external_id, env, dry_run, ctx.obj.verbose, CDFToolConfig.from_context(ctx), TransformationLoader
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
    source_dir: Annotated[
        str,
        typer.Argument(
            help="Where to find the destination module templates (project directory).",
            allow_dash=True,
        ),
    ] = "./",
    env: Annotated[
        str,
        typer.Option(
            "--env",
            "-e",
            help="Environment to use.",
        ),
    ] = "dev",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
) -> None:
    """This command will pull the specified node and update its YAML file in the module folder."""
    PullCommand().execute(
        source_dir,
        NodeId(space, external_id),
        env,
        dry_run,
        ctx.obj.verbose,
        CDFToolConfig.from_context(ctx),
        NodeLoader,
    )


@dump_app.callback(invoke_without_command=True)
def dump_main(ctx: typer.Context) -> None:
    """Commands to dump resource configurations from CDF into a temporary directory."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk dump --help[/] for more information.")
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
) -> None:
    """This command will dump the selected data model as yaml to the folder specified, defaults to /tmp."""
    cmd = DumpCommand()
    cmd.execute(
        CDFToolConfig.from_context(ctx),
        DataModelId(space, external_id, version),
        Path(output_dir),
        clean,
        ctx.obj.verbose,
    )


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
