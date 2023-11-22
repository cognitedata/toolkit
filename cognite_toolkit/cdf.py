#!/usr/bin/env python
import difflib
import shutil
import tempfile
import urllib
import zipfile
from dataclasses import dataclass
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import Annotated, Optional

import typer
from dotenv import load_dotenv
from rich import print
from rich.panel import Panel

from cognite_toolkit.cdf_tk import bootstrap
from cognite_toolkit.cdf_tk.delete import (
    delete_groups,
    delete_raw,
    delete_timeseries,
    delete_transformations,
)

# from scripts.delete import clean_out_datamodels
from cognite_toolkit.cdf_tk.load import (
    load_datamodel,
    load_groups,
    load_nodes,
    load_raw,
    load_timeseries_metadata,
    load_transformations,
)
from cognite_toolkit.cdf_tk.templates import build_config, read_environ_config
from cognite_toolkit.cdf_tk.utils import CDFToolConfig

app = typer.Typer(pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False)
auth_app = typer.Typer(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
)
app.add_typer(auth_app, name="auth")


# These are the supported data types for deploying to a CDF project.
# The enum matches the directory names that are expected in each module directory.
class CDFDataTypes(str, Enum):
    raw = "raw"
    timeseries = "timeseries"
    transformations = "transformations"
    data_models = "data_models"
    instances = "instances"
    groups = "groups"


# Common parameters handled in common callback
@dataclass
class Common:
    override_env: bool
    verbose: bool
    cluster: str
    project: str
    mockToolGlobals: CDFToolConfig


@app.callback(invoke_without_command=True)
def common(
    ctx: typer.Context,
    verbose: Annotated[
        bool,
        typer.Option(
            help="Turn on to get more verbose output",
        ),
    ] = False,
    override_env: Annotated[
        bool,
        typer.Option(
            help="Use .env file to override current environment variables",
        ),
    ] = False,
    cluster: Annotated[
        Optional[str],
        typer.Option(
            envvar="CDF_CLUSTER",
            help="Cognite Data Fusion cluster to use",
        ),
    ] = None,
    project: Annotated[
        Optional[str],
        typer.Option(
            envvar="CDF_PROJECT",
            help="Cognite Data Fusion project to use",
        ),
    ] = None,
):
    if ctx.invoked_subcommand is None:
        print(
            "[bold]A tool to manage and deploy Cognite Data Fusion project configurations from the command line or through CI/CD pipelines.[/]"
        )
        print("[bold yellow]Usage:[/] cdf-tk [OPTIONS] COMMAND [ARGS]...")
        print("       Use --help for more information.")
        return
    if override_env:
        print("  [bold red]WARNING:[/] Overriding environment variables with values from .env file...")
        if cluster is not None or project is not None:
            print("            --cluster or --project are set and will override .env file values.")
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
    if not Path(source_dir).is_dir() or not (Path(source_dir) / "local.yaml").is_file():
        print(f"  [bold red]ERROR:[/] {source_dir} does not exist or no local.yaml file found.")
        exit(1)
    print(
        Panel(
            f"[bold]Building config files from templates into {build_dir} for environment {build_env} using {source_dir} as sources...[/bold]"
        )
    )

    build_config(build_dir=build_dir, source_dir=source_dir, build_env=build_env, clean=clean)


@app.command("deploy")
def deploy(
    ctx: typer.Context,
    build_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to deploy from",
            allow_dash=True,
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
    interactive: Annotated[
        Optional[bool],
        typer.Option(
            "--interactive",
            "-i",
            help="Whether to use interactive mode when deciding which modules to deploy",
        ),
    ] = False,
    drop: Annotated[
        Optional[bool],
        typer.Option(
            "--drop",
            "-d",
            help="Whether to drop existing configurations, drop per resource if present",
        ),
    ] = False,
    drop_data: Annotated[
        Optional[bool],
        typer.Option(
            "--drop-data",
            "-D",
            help="Whether to drop existing data, drop data if present (WARNING!! includes data from pipelines)",
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
        Optional[list[CDFDataTypes]],
        typer.Option(
            "--include",
            "-i",
            help="Specify which resources to deploy",
        ),
    ] = None,
) -> None:
    """Deploy one or more configuration types from the built configrations to a CDF environment of your choice (as set in local.yaml)."""
    # Override cluster and project from the options/env variables
    if ctx.obj.mockToolGlobals is not None:
        ToolGlobals = ctx.obj.mockToolGlobals
    else:
        ToolGlobals = CDFToolConfig(
            client_name="cdf-project-templates",
            cluster=ctx.obj.cluster,
            project=ctx.obj.project,
        )
    # Set environment variables from local.yaml
    read_environ_config(root_dir=build_dir, build_env=build_env, set_env_only=True)
    if interactive:
        include: CDFDataTypes = []
        mapping = {}
        for i, datatype in enumerate(CDFDataTypes):
            print(f"[bold]{i})[/] {datatype.name}")
            mapping[i] = datatype
        print("\na) All")
        print("q) Quit")
        answer = input("Select data types to deploy: ")
        if answer.casefold() == "a":
            ...
        elif answer.casefold() == "q":
            exit(0)
        else:
            try:
                include = mapping[int(answer)]
            except ValueError:
                print(f"Invalid selection: {answer}")
                exit(1)
    else:
        if len(include) == 0:
            include = [datatype for datatype in CDFDataTypes]
    print(Panel(f"[bold]Deploying config files from {build_dir} to environment {build_env}...[/]"))
    # Configure a client and load credentials from environment
    if not Path(build_dir).is_dir():
        alternatives = {
            folder.name: f"{folder.parent.name}/{folder.name}"
            for folder in Path(build_dir).parent.iterdir()
            if folder.is_dir()
        }
        matches = difflib.get_close_matches(Path(build_dir).name, list(alternatives.keys()), n=3, cutoff=0.3)
        print(
            f"  [bold red]WARNING:[/] {build_dir} does not exists. Did you mean one of these? {[alternatives[m] for m in matches]}"
        )
        exit(1)
    print(ToolGlobals.as_string())
    if CDFDataTypes.raw in include and Path(f"{build_dir}/raw").is_dir():
        # load_raw() will assume that the RAW database name is set like this in the filename:
        # <index>.<raw_db>.<tablename>.csv
        load_raw(
            ToolGlobals,
            raw_db="default",
            drop=drop,
            file=None,
            dry_run=dry_run,
            directory=f"{build_dir}/raw",
        )
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to load RAW as expected.")
            exit(1)
    if CDFDataTypes.timeseries in include and Path(f"{build_dir}/timeseries").is_dir():
        load_timeseries_metadata(
            ToolGlobals,
            drop=drop,
            file=None,
            dry_run=dry_run,
            directory=f"{build_dir}/timeseries",
        )
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to load timeseries as expected.")
            exit(1)
    if CDFDataTypes.transformations in include and Path(f"{build_dir}/transformations").is_dir():
        load_transformations(
            ToolGlobals,
            file=None,
            drop=drop,
            dry_run=dry_run,
            directory=f"{build_dir}/transformations",
        )
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to load transformations as expected.")
            exit(1)
    if CDFDataTypes.data_models in include and (models_dir := Path(f"{build_dir}/data_models")).is_dir():
        load_datamodel(
            ToolGlobals,
            drop=drop,
            directory=models_dir,
            delete_containers=drop_data,  # Also delete properties that have been ingested (leaving empty instances)
            delete_spaces=drop_data,  # Also delete spaces if there are no empty instances (needs to be deleted separately)
            dry_run=dry_run,
        )
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to load data models as expected.")
            exit(1)
    if CDFDataTypes.instances in include and (models_dir := Path(f"{build_dir}/data_models")).is_dir():
        load_nodes(
            ToolGlobals,
            directory=models_dir,
            dry_run=dry_run,
        )
        if ToolGlobals.failed:
            print("[bold red]ERROR: [/] Failure to load instances as expected.")
            exit(1)
    if CDFDataTypes.groups in include and Path(f"{build_dir}/auth").is_dir():
        load_groups(
            ToolGlobals,
            directory=f"{build_dir}/auth",
            dry_run=dry_run,
            verbose=ctx.obj.verbose,
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to load as expected.")
        exit(1)


@app.command("clean")
def clean(
    ctx: typer.Context,
    build_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Where to find the module templates to clean from",
            allow_dash=True,
        ),
    ] = "./build",
    build_env: Annotated[
        Optional[str],
        typer.Option(
            "--env",
            "-e",
            help="Build environment to clean for",
        ),
    ] = "dev",
    dry_run: Annotated[
        Optional[bool],
        typer.Option(
            "--dry-run",
            "-r",
            help="Whether to do a dry-run, do dry-run if present",
        ),
    ] = False,
    include: Annotated[
        Optional[list[CDFDataTypes]],
        typer.Option(
            "--include",
            "-i",
            help="Specify which resources to deploy",
        ),
    ] = None,
) -> None:
    """Clean up a CDF environment as set in local.yaml based on the configuration files in the build directory."""
    if len(include) == 0:
        include = [datatype for datatype in CDFDataTypes]
    print(
        Panel(
            f"[bold]Cleaning configuration in project based on config files from {build_dir} to environment {build_env}...[/]"
        )
    )
    # Set environment variables from local.yaml
    read_environ_config(root_dir=build_dir, build_env=build_env, set_env_only=True)
    # Configure a client and load credentials from environment
    if not Path(build_dir).is_dir():
        print(f"{build_dir} does not exists.")
        exit(1)
    if ctx.obj.mockToolGlobals is not None:
        ToolGlobals = ctx.obj.mockToolGlobals
    else:
        ToolGlobals = CDFToolConfig(
            client_name="cdf-project-templates",
            cluster=ctx.obj.cluster,
            project=ctx.obj.project,
        )
    print("Using following configurations: ")
    print(ToolGlobals)
    if CDFDataTypes.raw in include and Path(f"{build_dir}/raw").is_dir():
        # load_raw() will assume that the RAW database name is set like this in the filename:
        # <index>.<raw_db>.<tablename>.csv
        delete_raw(
            ToolGlobals,
            raw_db="default",
            dry_run=dry_run,
            directory=f"{build_dir}/raw",
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to clean raw as expected.")
        exit(1)
    if CDFDataTypes.timeseries in include and Path(f"{build_dir}/timeseries").is_dir():
        delete_timeseries(
            ToolGlobals,
            dry_run=dry_run,
            directory=f"{build_dir}/timeseries",
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to clean timeseries as expected.")
        exit(1)
    if CDFDataTypes.transformations in include and Path(f"{build_dir}/transformations").is_dir():
        delete_transformations(
            ToolGlobals,
            dry_run=dry_run,
            directory=f"{build_dir}/transformations",
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to clean transformations as expected.")
        exit(1)
    if CDFDataTypes.data_models in include and (models_dir := Path(f"{build_dir}/data_models")).is_dir():
        # We use the load_datamodel with only_drop=True to ensure that we get a clean
        # deletion of the data model entities and instances.
        load_datamodel(
            ToolGlobals,
            drop=True,
            only_drop=True,
            directory=models_dir,
            delete_removed=True,
            delete_spaces=True,  # Also delete properties that have been ingested (leaving empty instances)
            delete_containers=True,  # Also delete spaces if there are no empty instances (needs to be deleted separately)
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to delete data models as expected.")
        exit(1)
    if CDFDataTypes.groups in include and Path(f"{build_dir}/auth").is_dir():
        # NOTE! If you want to force deletion of groups that the current running user/service principal
        # is a member of, set my_own=True. This may result in locking out the CI/CD service principal
        # and is thus default not set to True.
        delete_groups(
            ToolGlobals,
            directory=f"{build_dir}/auth",
            my_own=False,
            dry_run=dry_run,
        )
    if ToolGlobals.failed:
        print("[bold red]ERROR: [/] Failure to clean groups as expected.")
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
            help="Whether to do a dry-run, do dry-run if present",
        ),
    ] = False,
    interactive: Annotated[
        Optional[bool],
        typer.Option(
            "--interactive",
            "-i",
            help="Will run the verification in interactive mode, prompting for input",
        ),
    ] = False,
    group_file: Annotated[
        Optional[str],
        typer.Option(
            "--group-file",
            "-f",
            help="Group yaml configuration file to use for group verification",
        ),
    ] = "/common/cdf_auth_readwrite_all/auth/readwrite.all.group.yaml",
    update_group: Annotated[
        Optional[int],
        typer.Option(
            "--update-group",
            "-u",
            help="Used to update an existing group with the configurations from the configuration file. Set to the group id to update or 1 to update the only available group",
        ),
    ] = 0,
    create_group: Annotated[
        Optional[str],
        typer.Option(
            "--create-group",
            "-c",
            help="Used to create a new group with the configurations from the configuration file. Set to the source id that the new group should be configured with",
        ),
    ] = None,
):
    """When you have a CDF_TOKEN or a pair of CDF_CLIENT_ID and CDF_CLIENT_SECRET for a CDF project,
    you can use this command to verify that the token has the correct access rights to the project.
    It can also create a group with the correct access rights, defaulting to write-all group
    meant for an admin/CICD pipeline.

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
        ToolGlobals = CDFToolConfig(
            client_name="cdf-project-templates",
            cluster=ctx.obj.cluster,
            project=ctx.obj.project,
        )
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
            help="Whether to do a dry-run, do dry-run if present",
        ),
    ] = False,
    upgrade: Annotated[
        Optional[bool],
        typer.Option(
            "--upgrade",
            "-u",
            help="Will upgrade templates in place without overwriting config.yaml files",
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
            help="Will skip making a backup before upgrading",
        ),
    ] = False,
    clean: Annotated[
        Optional[bool],
        typer.Option(
            "--clean",
            hidden=True,
            help="Will delete the new_project directory before starting",
        ),
    ] = False,
    init_dir: Annotated[
        Optional[str],
        typer.Argument(
            help="Directory to initialize with templates",
        ),
    ] = "new_project",
):
    """Initialize a new CDF project with templates."""

    files_to_copy = [
        "default.config.yaml",
        "default.packages.yaml",
    ]
    dirs_to_copy = []
    if not upgrade:
        files_to_copy.extend(
            [
                "config.yaml",
                "local.yaml",
                "packages.yaml",
                "README.md",
                ".gitignore",
                ".env.tmpl",
            ]
        )
        dirs_to_copy.append("local_modules")
    module_dirs_to_copy = [
        "common",
        "modules",
        "examples",
        "experimental",
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
        zip = f"https://github.com/cognitedata/cdf-project-templates/archive/refs/heads/{git}.zip"
        extract_dir = tempfile.mkdtemp(prefix="git.", suffix=".tmp", dir=Path.cwd())
        print(f"Upgrading templates from https://github.com/cognitedata/cdf-project-templates, branch {git}...")
        print(
            "  [bold yellow]WARNING:[/] You are only upgrading templates, not the cdf-tk tool. Your current version may not support the new templates."
        )
        if not dry_run:
            try:
                zip_path, _ = urllib.request.urlretrieve(zip)
                with zipfile.ZipFile(zip_path, "r") as f:
                    f.extractall(extract_dir)
            except Exception as e:
                print(
                    f"Failed to download templates. Are you sure that the branch {git} exists in the repository?\n{e}"
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
            print(f"Project in {target_dir} was upgraded.")
        else:
            print(f"New project created in {target_dir}.")
        if upgrade:
            print("  All default.config.yaml files in the modules have been upgraded.")
            print("  Your config.yaml files may need to be updated to override new default variales.")


if __name__ == "__main__":
    app()
