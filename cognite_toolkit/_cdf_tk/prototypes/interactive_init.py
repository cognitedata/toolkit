from typing import Annotated, Optional

import typer


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def interactive(
        self,
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
        """Initialize or upgrade a new CDF project with templates interactively."""

        print("Initializing or upgrading a new CDF project with templates interactively.")
        typer.Exit()


command = InteractiveInit(
    name="init", help="Initialize or upgrade a new CDF project with templates interactively."
).interactive
