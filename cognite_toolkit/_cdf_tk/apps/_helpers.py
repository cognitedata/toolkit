import typer


def print_help_if_no_subcommand(ctx: typer.Context) -> None:
    """Print this group's help and exit when no subcommand was given."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()
