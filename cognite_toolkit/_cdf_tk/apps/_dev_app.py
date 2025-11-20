from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import ResourcesCommand

from ._run import RunApp

CDF_TOML = CDFToml.load(Path.cwd())


class DevApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(RunApp(*args, **kwargs), name="run")
        self.command("create")(self.create)
        self.command("icreate")(self.create_interactive)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with development."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev --help[/] for more information.")
        return None

    def create(
        self,
        module: Annotated[
            str,
            typer.Argument(
                help="Name of an existing module or a new module to create the resource in.",
            ),
        ],
        resource_directory: Annotated[
            str,
            typer.Argument(
                help="The resource directory to create the resources in. eg. data_models, functions, workflows, etc.",
            ),
        ],
        resources: Annotated[
            list[str],
            typer.Option(
                "--resources",
                "-r",
                help="The resources to create under the resource directories. eg. if resource_directory is data_models, then --resources=container,space,view,datamodel, etc. If not provided, all resources will be created.",
                callback=lambda ctx, param, value: (
                    [
                        item.strip()
                        for _r in (value if isinstance(value, list) else [value] if value else [])
                        for item in (_r.replace(" ", "").split(",") if isinstance(_r, str) else [])
                        if item.strip()
                    ]
                    if value
                    else []
                ),
            ),
        ] = [],
        file_names: Annotated[
            list[str],
            typer.Option(
                "--file-names",
                "-f",
                help="The name of the resource file to create without suffixes and extensions. Comma-separated values are supported. eg. --file-names=file1,file2,file3. If not provided, default file names will be used eg. 'my_agent'.",
                callback=lambda ctx, param, value: (
                    [
                        item.strip()
                        for _fn in (value if isinstance(value, list) else [value] if value else [])
                        for item in (_fn.replace(" ", "").split(",") if isinstance(_fn, str) else [])
                        if item.strip()
                    ]
                    if value
                    else []
                ),
            ),
        ] = [],
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to the organization directory",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
    ) -> None:
        """create resource YAMLs."""
        if file_names and len(resources) != len(file_names):
            raise typer.BadParameter("Number of resources must match number of file names.")

        cmd = ResourcesCommand()
        cmd.run(
            lambda: cmd.create(
                organization_dir=organization_dir,
                module_name=module,
                resource_directory=resource_directory,
                resources=resources if resources else None,
                file_name=file_names if file_names else None,
                verbose=verbose,
            )
        )

    def create_interactive(
        self,
        module: Annotated[
            str,
            typer.Argument(
                help="Name of an existing module or a new module to create the resource in.",
            ),
        ],
        resource_directories: Annotated[
            list[str],
            typer.Option(
                "--resource-directories",
                "-d",
                help="The resource directories to create the resources in. eg. -d data_models,functions,workflows, etc.",
                callback=lambda ctx, param, value: (
                    [
                        item.strip()
                        for _rd in (value if isinstance(value, list) else [value] if value else [])
                        for item in (_rd.replace(" ", "").split(",") if isinstance(_rd, str) else [])
                        if item.strip()
                    ]
                    if value
                    else []
                ),
            ),
        ] = [],
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Path to the organization directory",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
    ) -> None:
        """create resource YAMLs using interactive prompts if arguments are not provided."""
        cmd = ResourcesCommand()
        cmd.run(
            lambda: cmd.create_interactive(
                organization_dir=organization_dir,
                module_name=module,
                resource_directories=resource_directories,
                verbose=verbose,
            )
        )
