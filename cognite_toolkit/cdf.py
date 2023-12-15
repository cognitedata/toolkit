#!/usr/bin/env python
import shutil
import tempfile
import urllib
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from graphlib import TopologicalSorter
from importlib import resources
from pathlib import Path
from typing import Annotated, Optional

import typer
from dotenv import load_dotenv
from rich import print
from rich.panel import Panel

from cognite_toolkit import _version
from cognite_toolkit.cdf_tk import bootstrap
from cognite_toolkit.cdf_tk.load import (
    LOADER_BY_FOLDER_NAME,
    AuthLoader,
    DeployResults,
    deploy_or_clean_resources,
)
from cognite_toolkit.cdf_tk.templates import (
    BUILD_ENVIRONMENT_FILE,
    COGNITE_MODULES,
    CONFIG_FILE,
    CUSTOM_MODULES,
    ENVIRONMENTS_FILE,
    BuildEnvironment,
    build_config,
    generate_config,
    read_yaml_file,
)
from cognite_toolkit.cdf_tk.utils import CDFToolConfig

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)
auth_app = typer.Typer(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
)
app.add_typer(auth_app, name="auth")


_AVAILABLE_DATA_TYPES: tuple[str, ...] = tuple(LOADER_BY_FOLDER_NAME)


# Common parameters handled in common callback
@dataclass
class Common:
    override_env: bool
    verbose: bool
    cluster: str
    project: str
    mockToolGlobals: CDFToolConfig


def _version_callback(value: bool):
    if value:
        typer.echo(f"CDF-Toolkit version: {_version.__version__}.")
        raise typer.Exit()


@app.callback(invoke_without_command=True)
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
    version: bool = typer.Option(
        None,
        "--version",
        help="See which version of the tooklit and the templates are installed.",
        callback=_version_callback,
    ),
):
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
    if not (Path.cwd() / ".env").is_file():
        if not (Path.cwd().parent / ".env").is_file():
            print("[bold yellow]WARNING:[/] No .env file found in current or parent directory.")
        else:
            if verbose:
                print("Loading .env file found in parent directory.")
            load_dotenv("../.env", override=override_env)
    else:
        if verbose:
            print("Loading .env file found in current directory.")
        load_dotenv(".env", override=override_env)
    ctx.obj = Common(
        verbose=verbose,
        override_env=override_env,
        cluster=cluster,
        project=project,
        mockToolGlobals=None,
    )


@app.command("build")
def build(
    ctx: typer.Context,
    source_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to build from",
            allow_dash=True,
        ),
    ] = "./",
    build_dir: Annotated[
        Optional[str],
        typer.Option(
            "--build-dir",
            "-b",
            help="Where to save the built module files",
        ),
    ] = "./build",
    build_env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="Build environment to build for",
        ),
    ] = "dev",
    clean: Annotated[
        Optional[bool],
        typer.Option(
            "--clean",
            "-c",
            help="Delete the build directory before building the configurations",
        ),
    ] = False,
) -> None:
    """Build configuration files from the module templates to a local build directory."""
    source_dir = Path(source_dir)
    if not source_dir.is_dir():
        print(f"  [bold red]ERROR:[/] {source_dir} does not exist")
        exit(1)
    environment_file = Path.cwd() / ENVIRONMENTS_FILE
    if not environment_file.is_file() and not (environment_file := source_dir / ENVIRONMENTS_FILE).is_file():
        print(f"  [bold red]ERROR:[/] {environment_file} does not exist")
        exit(1)
    config_file = Path.cwd() / CONFIG_FILE
    if not config_file.is_file() and not (config_file := source_dir / CONFIG_FILE).is_file():
        print(f"  [bold red]ERROR:[/] {config_file} does not exist")
        exit(1)
    print(
        Panel(
            f"[bold]Building config files from templates into {build_dir!s} for environment {build_env} using {source_dir!s} as sources...[/bold]"
            f"\n[bold]Environment file:[/] {environment_file.absolute().relative_to(Path.cwd())!s} and [bold]config file:[/] {config_file.absolute().relative_to(Path.cwd())!s}"
        )
    )
    print(f"  Environment is {build_env}, using that section in {ENVIRONMENTS_FILE!s}.\n")
    build_ = BuildEnvironment.load(read_yaml_file(environment_file), build_env, "build")
    build_.set_environment_variables()

    build_config(
        build_dir=Path(build_dir),
        source_dir=source_dir,
        config_file=config_file,
        build=build_,
        clean=clean,
        verbose=ctx.obj.verbose,
    )


@app.command("deploy")
def deploy(
    ctx: typer.Context,
    build_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to deploy from. Defaults to current directory.",
            allow_dash=True,
        ),
    ] = "./build",
    build_env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to build for. Defined in environments.yaml. Defaults to dev.",
        ),
    ] = "dev",
    interactive: Annotated[
        Optional[bool],
        typer.Option(
            "--interactive",
            "-i",
            help="Whether to use interactive mode when deciding which modules to deploy.",
        ),
    ] = False,
    drop: Annotated[
        Optional[bool],
        typer.Option(
            "--drop",
            "-d",
            help="Whether to drop existing configurations, drop per resource if present.",
        ),
    ] = False,
    drop_data: Annotated[
        Optional[bool],
        typer.Option(
            "--drop-data",
            help="Whether to drop existing data in data model containers and spaces.",
        ),
    ] = False,
    dry_run: Annotated[
        Optional[bool],
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
    """Deploy one or more resource types from the built configurations to a CDF project environment of your choice (as set in environments.yaml)."""
    # Override cluster and project from the options/env variables
    if ctx.obj.mockToolGlobals is not None:
        ToolGlobals = ctx.obj.mockToolGlobals
    else:
        ToolGlobals = CDFToolConfig(cluster=ctx.obj.cluster, project=ctx.obj.project)

    build_ = BuildEnvironment.load(read_yaml_file(Path(build_dir) / BUILD_ENVIRONMENT_FILE), build_env, "deploy")
    build_.set_environment_variables()

    print(Panel(f"[bold]Deploying config files from {build_dir} to environment {build_env}...[/]"))
    build_path = Path(build_dir)
    if not build_path.is_dir():
        typer.echo(
            f"  [bold yellow]WARNING:[/] {build_dir} does not exists. Did you forget to run `cdf-tk build` first?"
        )
        exit(1)

    include = _process_include(include, interactive)
    print(ToolGlobals.as_string())

    # The 'auth' loader is excluded, as it is run twice,
    # once with all_scoped_skipped_validation and once with resource_scoped_only
    selected_loaders = {
        LoaderCls: LoaderCls.dependencies
        for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items()
        if folder_name in include and folder_name != "auth" and (build_path / folder_name).is_dir()
        for LoaderCls in loader_classes
    }

    arguments = dict(
        ToolGlobals=ToolGlobals,
        drop=drop,
        action="deploy",
        dry_run=dry_run,
        drop_data=drop_data,
        verbose=ctx.obj.verbose,
    )
    results = DeployResults([], "deploy", dry_run=dry_run)
    if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
        # First, we need to get all the generic access, so we can create the rest of the resources.
        print("[bold]EVALUATING auth resources (groups) with ALL scope...[/]")
        result = deploy_or_clean_resources(
            AuthLoader.create_loader(ToolGlobals, target_scopes="all_scoped_skipped_validation"),
            directory,
            **arguments,
        )
        results.append(result)
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to deploy auth (groups) with ALL scope as expected.")
            exit(1)
    for LoaderCls in TopologicalSorter(selected_loaders).static_order():
        result = deploy_or_clean_resources(
            LoaderCls.create_loader(ToolGlobals),
            build_path / LoaderCls.folder_name,
            **arguments,
        )
        results.append(result)
        if ToolGlobals.failed:
            if results:
                print(results.create_rich_table())
            print(f"[bold red]ERROR: [/] Failure to load {LoaderCls.folder_name} as expected.")
            exit(1)

    if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
        # Last, we need to get all the scoped access, as the resources should now have been created.
        print("[bold]EVALUATING auth resources scoped to resources...[/]")
        result = deploy_or_clean_resources(
            AuthLoader.create_loader(ToolGlobals, target_scopes="resource_scoped_only"),
            directory,
            **arguments,
        )
        results.append(result)
    print(results.create_rich_table())
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to deploy auth (groups) scoped to resources as expected.")
        exit(1)


@app.command("clean")
def clean(
    ctx: typer.Context,
    build_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to clean from. Defaults to ./build directory.",
            allow_dash=True,
        ),
    ] = "./build",
    build_env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="CDF project environment to use for cleaning.",
        ),
    ] = "dev",
    interactive: Annotated[
        Optional[bool],
        typer.Option(
            "--interactive",
            "-i",
            help="Whether to use interactive mode when deciding which resource types to clean.",
        ),
    ] = False,
    dry_run: Annotated[
        Optional[bool],
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
    if ctx.obj.mockToolGlobals is not None:
        ToolGlobals = ctx.obj.mockToolGlobals
    else:
        ToolGlobals = CDFToolConfig(cluster=ctx.obj.cluster, project=ctx.obj.project)

    build_ = BuildEnvironment.load(read_yaml_file(Path(build_dir) / BUILD_ENVIRONMENT_FILE), build_env, "clean")
    build_.set_environment_variables()

    Panel(f"[bold]Cleaning environment {build_env} based on config files from {build_dir}...[/]")
    build_path = Path(build_dir)
    if not build_path.is_dir():
        typer.echo(
            f"  [bold yellow]WARNING:[/] {build_dir} does not exists. Did you forget to run `cdf-tk build` first?"
        )
        exit(1)

    include = _process_include(include, interactive)
    print(ToolGlobals.as_string())

    # The 'auth' loader is excluded, as it is run at the end.
    selected_loaders = {
        LoaderCls: LoaderCls.dependencies
        for folder_name, loader_classes in LOADER_BY_FOLDER_NAME.items()
        if folder_name in include and folder_name != "auth" and (build_path / folder_name).is_dir()
        for LoaderCls in loader_classes
    }

    print(ToolGlobals.as_string())
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to delete data models as expected.")
        exit(1)
    results = DeployResults([], "clean", dry_run=dry_run)
    for LoaderCls in reversed(list(TopologicalSorter(selected_loaders).static_order())):
        result = deploy_or_clean_resources(
            LoaderCls.create_loader(ToolGlobals),
            build_path / LoaderCls.folder_name,
            ToolGlobals,
            drop=True,
            action="clean",
            drop_data=True,
            dry_run=dry_run,
            verbose=ctx.obj.verbose,
        )
        results.append(result)
        if ToolGlobals.failed:
            if results:
                print(results.create_rich_table())
            print(f"[bold red]ERROR: [/] Failure to clean {LoaderCls.folder_name} as expected.")
            exit(1)
    if "auth" in include and (directory := (Path(build_dir) / "auth")).is_dir():
        result = deploy_or_clean_resources(
            AuthLoader.create_loader(ToolGlobals, target_scopes="all"),
            directory,
            ToolGlobals,
            drop=True,
            clean=True,
            action="clean",
            dry_run=dry_run,
            verbose=ctx.obj.verbose,
        )
        results.append(result)
    print(results.create_rich_table())
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to clean auth as expected.")
        exit(1)


@auth_app.callback(invoke_without_command=True)
def auth_main(ctx: typer.Context):
    """Test, validate, and configure authentication and authorization for CDF projects."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk auth --help[/] for more information.")


@auth_app.command("verify")
def auth_verify(
    ctx: typer.Context,
    dry_run: Annotated[
        Optional[bool],
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    interactive: Annotated[
        Optional[bool],
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
            help="Path to group yaml configuration file to use for group verification. Defaults to readwrite.all.group.yaml from the cdf_auth_readwrite_all common module.",
        ),
    ] = f"/{COGNITE_MODULES}/common/cdf_auth_readwrite_all/auth/readwrite.all.group.yaml",
    update_group: Annotated[
        Optional[int],
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
):
    """When you have the necessary information about your identity provider configuration,
    you can use this command to configure the tool and verify that the token has the correct access rights to the project.
    It can also create a group with the correct access rights, defaulting to write-all group
    meant for an admin/CICD pipeline.

    As a minimum, you need the CDF project name, the CDF cluster, an identity provider token URL, and a service account client ID
    and client secret (or an OAuth2 token set in CDF_TOKEN environment variable).

    Needed capabilites for bootstrapping:
    "projectsAcl": ["LIST", "READ"],
    "groupsAcl": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"]

    The default bootstrap group configuration is readwrite.all.group.yaml from the cdf_auth_readwrite_all common module.
    """
    if create_group is not None and update_group != 0:
        print("[bold red]ERROR: [/] --create-group and --update-group are mutually exclusive.")
        exit(1)
    if ctx.obj.mockToolGlobals is not None:
        ToolGlobals = ctx.obj.mockToolGlobals
    else:
        ToolGlobals = CDFToolConfig(cluster=ctx.obj.cluster, project=ctx.obj.project)
    bootstrap.check_auth(
        ToolGlobals,
        group_file=group_file,
        update_group=update_group,
        create_group=create_group,
        interactive=interactive,
        dry_run=dry_run,
        verbose=ctx.obj.verbose,
    )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to verify access rights.")
        exit(1)


@app.command("init")
def main_init(
    ctx: typer.Context,
    dry_run: Annotated[
        Optional[bool],
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present.",
        ),
    ] = False,
    upgrade: Annotated[
        Optional[bool],
        typer.Option(
            "--upgrade",
            "-u",
            help="Will upgrade templates in place without overwriting existing config.yaml and other files.",
        ),
    ] = False,
    git: Annotated[
        Optional[str],
        typer.Option(
            "--git",
            "-g",
            help="Will download the latest templates from the git repository branch specified. Use `main` to get the very latest templates.",
        ),
    ] = None,
    no_backup: Annotated[
        Optional[bool],
        typer.Option(
            "--no-backup",
            help="Will skip making a backup before upgrading.",
        ),
    ] = False,
    clean: Annotated[
        Optional[bool],
        typer.Option(
            "--clean",
            help="Will delete the new_project directory before starting.",
        ),
    ] = False,
    init_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Directory path to project to initialize or upgrade with templates.",
        ),
    ] = "new_project",
):
    """Initialize or upgrade a new CDF project with templates."""

    files_to_copy = []
    dirs_to_copy = []
    if not upgrade:
        files_to_copy.extend(
            [
                "environments.yaml",
                "README.md",
                ".gitignore",
                ".env.tmpl",
            ]
        )
        dirs_to_copy.append(CUSTOM_MODULES)
    module_dirs_to_copy = [
        COGNITE_MODULES,
    ]
    template_dir = resources.files("cognite_toolkit")
    target_dir = Path.cwd() / f"{init_dir}"
    if target_dir.exists():
        if not upgrade:
            if clean:
                if dry_run:
                    print(f"Would clean out directory {target_dir}...")
                else:
                    print(f"Cleaning out directory {target_dir}...")
                    shutil.rmtree(target_dir)
            else:
                print(f"Directory {target_dir} already exists.")
                exit(1)
        else:
            print(f"[bold]Upgrading directory {target_dir}...[/b]")
    elif upgrade:
        print(f"Found no directory {target_dir} to upgrade.")
        exit(1)
    if not dry_run and not upgrade:
        target_dir.mkdir(exist_ok=True)
    if upgrade:
        print("  Will upgrade modules and files in place, config.yaml files will not be touched.")
    print(f"Will copy these files to {target_dir}:")
    print(files_to_copy)
    print(f"Will copy these module directories to {target_dir}:")
    print(module_dirs_to_copy)
    print(f"Will copy these directories to {target_dir}:")
    print(dirs_to_copy)
    extract_dir = None
    if upgrade and git is not None:
        toolkit_github_url = f"https://github.com/cognitedata/cdf-project-templates/archive/refs/heads/{git}.zip"
        extract_dir = tempfile.mkdtemp(prefix="git.", suffix=".tmp", dir=Path.cwd())
        print(f"Upgrading templates from https://github.com/cognitedata/cdf-project-templates, branch {git}...")
        print(
            "  [bold yellow]WARNING:[/] You are only upgrading templates, not the cdf-tk tool. Your current version may not support the new templates."
        )
        if not dry_run:
            try:
                zip_path, _ = urllib.request.urlretrieve(toolkit_github_url)
                with zipfile.ZipFile(zip_path, "r") as f:
                    f.extractall(extract_dir)
            except Exception:
                print(
                    f"Failed to download templates. Are you sure that the branch {git} exists in"
                    + "the https://github.com/cognitedata/cdf-project-templatesrepository?\n{e}"
                )
                exit(1)
        template_dir = Path(extract_dir) / f"cdf-project-templates-{git}" / "cognite_toolkit"
    for f in files_to_copy:
        if dry_run and ctx.obj.verbose:
            print("Would copy file", f, "to", target_dir)
        elif not dry_run:
            if ctx.obj.verbose:
                print("Copying file", f, "to", target_dir)
            shutil.copyfile(Path(template_dir) / f, target_dir / f)
    for d in dirs_to_copy:
        if dry_run and ctx.obj.verbose:
            if upgrade:
                print("Would copy and overwrite directory", d, "to", target_dir)
            else:
                print("Would copy directory", d, "to", target_dir)
        elif not dry_run:
            if ctx.obj.verbose:
                print("Copying directory", d, "to", target_dir)
            shutil.copytree(Path(template_dir) / d, target_dir / d, dirs_exist_ok=True)
    if upgrade and not no_backup:
        if dry_run:
            if ctx.obj.verbose:
                print(f"Would have backed up {target_dir}")
        else:
            backup_dir = tempfile.mkdtemp(prefix=f"{target_dir.name}.", suffix=".bck", dir=Path.cwd())
            if ctx.obj.verbose:
                print(f"Backing up {target_dir} to {backup_dir}...")
            shutil.copytree(Path(target_dir), Path(backup_dir), dirs_exist_ok=True)
    elif upgrade:
        print("[bold yellow]WARNING:[/] --no-backup is specified, no backup will be made.")
    for d in module_dirs_to_copy:
        if not dry_run:
            (Path(target_dir) / d).mkdir(exist_ok=True)
        if ctx.obj.verbose:
            if dry_run:
                print(f"Would have copied modules in {d}")
            else:
                print(f"Copying modules in {d}...")
        if not dry_run:
            shutil.copytree(Path(template_dir / d), target_dir / d, dirs_exist_ok=True)
    if extract_dir is not None:
        shutil.rmtree(extract_dir)
    if not dry_run:
        if upgrade:
            print(f"You project in {target_dir} was upgraded.")
        else:
            print(f"A new project was created in {target_dir}.")
        if upgrade:
            print("  All default variables from the modules have been upgraded.")
            print("  Please check you config.yaml file for new default variables that may need to be changed.")

    config_filepath = target_dir / "config.yaml"
    if not dry_run:
        if clean or not config_filepath.exists():
            config_str, _ = generate_config(target_dir)
            config_filepath.write_text(config_str)
            print(f"Created your config.yaml file in {target_dir}.")
        else:
            current = config_filepath.read_text()
            config_str, difference = generate_config(target_dir, existing_config=current)
            config_filepath.write_text(config_str)
            print(str(difference))


def _process_include(include: Optional[list[str]], interactive: bool) -> list[str]:
    if include and (invalid_types := set(include).difference(_AVAILABLE_DATA_TYPES)):
        print(
            f"  [bold red]ERROR:[/] Invalid resource types specified: {invalid_types}, available types: {_AVAILABLE_DATA_TYPES}"
        )
        exit(1)
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
        exit(0)
    else:
        try:
            return [mapping[int(answer)]]
        except ValueError:
            print(f"Invalid selection: {answer}")
            exit(1)


if __name__ == "__main__":
    app()
