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

    @staticmethod
    def main(ctx: typer.Context) -> None:
        """Commands to work with development."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dev --help[/] for more information.")
        return None

    def create(
        self,
        kind: Annotated[
            list[str],
            typer.Argument(
                help="The kind of resource to create. eg. container, space, view, datamodel, etc.",
                callback=lambda ctx, param, value: (
                    [
                        item.strip()
                        for _r in (value if isinstance(value, (list, tuple)) else [value] if value else [])
                        for item in (_r.replace(" ", "").split(",") if isinstance(_r, str) else [])
                        if item.strip()
                    ]
                    if value
                    else []
                ),
            ),
        ] = [],
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
                "eg. --prefix=my_space. If not provided, default prefix will be used eg. 'my_'.",
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
        cmd = ResourcesCommand()
        cmd.run(
            lambda: cmd.create(
                organization_dir=organization_dir,
                module_name=module if module else None,
                kind=kind if kind else None,
                prefix=prefix if prefix else None,
                verbose=verbose,
            )
        )
