import contextlib
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import ResourcesCommand
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from ._run import RunApp

CDF_TOML = CDFToml.load(Path.cwd())


class DevApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.add_typer(RunApp(*args, **kwargs), name="run")
        if FeatureFlag.is_enabled(Flags.CREATE):
            self.command("create")(self.create)

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with development."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev --help[/] for more information.")
        return None

    def create(
        self,
        kind: Annotated[
            list[str] | None,
            typer.Argument(
                help="The kind of resource to create. eg. container, space, view, datamodel, etc.",
                callback=lambda ctx, param, value: [
                    s.strip() for item in value or [] for s in item.split(",") if s.strip()
                ],
            ),
        ] = None,
        module: Annotated[
            str | None,
            typer.Option(
                "--module",
                "-m",
                help="Name of an existing module or a new module to create the resource in.",
            ),
        ] = None,
        prefix: Annotated[
            str | None,
            typer.Option(
                "--prefix",
                "-p",
                help="The prefix of the resource file to create without suffixes and extensions. "
                "eg. --prefix=my_space. If not provided, a default prefix like 'my_<kind>' will be used.",
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
        client: ToolkitClient | None = None
        with contextlib.redirect_stdout(None), contextlib.suppress(Exception):
            # Try to load client if possible, but ignore errors.
            # This is only used for logging purposes in the command.
            client = EnvironmentVariables.create_from_environment().get_client()

        cmd = ResourcesCommand(client=client)
        cmd.run(
            lambda: cmd.create(
                organization_dir=organization_dir,
                module_name=module,
                kind=kind,
                prefix=prefix,
                verbose=verbose,
            )
        )
