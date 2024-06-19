import typer

from .commands.import_ import ImportTransformationCLI

import_app = typer.Typer(
    pretty_exceptions_short=False, pretty_exceptions_show_locals=False, pretty_exceptions_enable=False
)


@import_app.callback(invoke_without_command=True)
def import_main(ctx: typer.Context) -> None:
    """Import resources into Cognite-Toolkit."""
    if ctx.invoked_subcommand is None:
        print("Use [bold yellow]cdf-tk import --help[/] for more information.")
    return None


@import_app.command("transformation-cli")
def transformation_cli() -> None:
    """Import transformation CLI manifests into Cognite-Toolkit modules."""
    cmd = ImportTransformationCLI()
    cmd.execute()
