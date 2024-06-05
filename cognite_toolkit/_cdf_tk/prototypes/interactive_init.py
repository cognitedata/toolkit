from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Optional, Union

import questionary
import typer
from rich import print
from rich.padding import Padding
from rich.panel import Panel
from rich.tree import Tree

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError

INDENT = "  "
POINTER = INDENT + "▶"

custom_style_fancy = questionary.Style(
    [
        ("qmark", "fg:#673ab7 bold"),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", "fg:#f44336 bold"),  # submitted answer text behind the question
        ("pointer", "fg:#673ab7 bold"),  # pointer used in select and checkbox prompts
        ("highlighted", "fg:#673ab7 bold"),  # pointed-at choice in select and checkbox prompts
        ("selected", "fg:#673ab7"),  # style for a selected item of a checkbox
        ("separator", "fg:#cc5454"),  # separator in lists
        ("instruction", ""),  # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        ("disabled", "fg:#858585 italic"),  # disabled choices for select and checkbox prompts
    ]
)


def get_packages() -> dict[str, dict[str, Any]]:
    return {
        "quickstart": {
            "title": "Quick Start: A set of modules for a CDF quick start project.",
            "items": {
                "sap_data_pipeline": {
                    "title": "SAP Data Pipeline",
                },
                "pi_data_pipeline": {
                    "title": "PI Data Pipeline",
                },
                "mqtt_data_pipeline": {
                    "title": "MQTT Data Pipeline",
                },
                "files_contextualization": {
                    "title": "Files Contextualization",
                },
                "asset_data_transformation": {
                    "title": "Asset Data Transformation",
                },
                "infield": {
                    "title": "Infield",
                },
            },
        },
        "examples": {
            "title": "Examples: a set of example modules for inspiration",
            "items": {
                "cdf_data_pipeline_asset_valhall": {"items": {}},
                "cdf_data_pipeline_files_valhall": {"items": {}},
            },
        },
        "reference": {
            "title": "All supported resources as reference",
            "items": {"workflow": {}, "transformations": {}, "functions": {}, "groups": {}},
        },
    }


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def build_tree(self, item: Union[dict, list], tree: Tree) -> None:
        if isinstance(item, dict):
            for key, value in item.items():
                subtree = tree.add(key)
                for subvalue in value:
                    if isinstance(subvalue, dict):
                        self.build_tree(subvalue, subtree)
                    else:
                        subtree.add(subvalue)

    def create(self, init_dir: str, selected: dict[str, list[str]], mode: str | None) -> None:
        pass

    def interactive(
        self,
        ctx: typer.Context,
        init_dir: Annotated[
            Optional[str],
            typer.Option(
                help="Directory path to project to initialize or upgrade with templates.",
            ),
        ] = None,
        arg_selected: Annotated[
            Optional[str],
            typer.Option(
                help=f"List of modules to include. Options are '{list(get_packages().keys())}'",
            ),
        ] = None,
        numeric: Annotated[
            Optional[bool],
            typer.Option(
                help="Use numeric selection instead of arrow keys.",
            ),
        ] = False,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        print("\n")
        print(
            Panel(
                "This wizard will help you prepare modules in the folder you enter. The modules are thematically bundled in packages you can choose between.",
                title="Interactive template wizard",
                style="green",
                padding=(1, 2),
            )
        )

        selected: dict[str, Any] = {}
        available = get_packages()
        mode = "new"

        if not init_dir:
            init_dir = questionary.text(
                "Which directory would you like to create templates in? (typically customer name)",
                default="new_project",
            ).ask()

        if init_dir and Path(init_dir).is_dir():
            if numeric:
                mode = questionary.rawselect(
                    "Directory already exists. What would you like to do?",
                    choices=[
                        questionary.Choice("Abort", "abort"),
                        questionary.Choice("Overwrite (clean existing)", "overwrite"),
                        questionary.Choice("Update (add to or replace existing)", "update"),
                    ],
                    pointer=POINTER,
                    style=custom_style_fancy,
                    instruction="(Press 1, 2 or 3)",
                ).ask()
            else:
                questionary.select(
                    "Directory already exists. What would you like to do?",
                    choices=[
                        questionary.Choice("Abort", "abort"),
                        questionary.Choice("Overwrite (clean existing)", "overwrite"),
                        questionary.Choice("Update (add to or replace existing)", "update"),
                    ],
                    pointer=POINTER,
                    style=custom_style_fancy,
                    instruction="use arrow up/down and " + "⮐ " + " to save",
                ).ask()
            if mode == "abort":
                print("Aborting...")
                raise typer.Exit()

        if not init_dir:
            raise ToolkitRequiredValueError("Directory path is required.")

        print(f"  [{'yellow' if mode == 'overwrite' else 'green'}]Using directory [bold]{init_dir}[/]")

        loop = True
        while loop:
            if not arg_selected:
                if numeric:
                    package_id = questionary.rawselect(
                        "Which package would you like to include?",
                        instruction="Type the number of your choice and press enter",
                        choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                        pointer=POINTER,
                        style=custom_style_fancy,
                    ).ask()

                else:
                    package_id = questionary.select(
                        "Which package would you like to include?",
                        instruction="Use arrow up/down and ⮐  to save",
                        choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                        pointer=POINTER,
                        style=custom_style_fancy,
                    ).ask()

                if package_id:
                    selected[package_id] = []
                    selection = questionary.checkbox(
                        f"Which modules of {package_id} would you like to include?",
                        instruction="Use arrow up/down, press space to select item(s) and enter to save",
                        choices=[
                            questionary.Choice(value.get("title", key), key)
                            for key, value in available[package_id].get("items", {}).items()
                        ],
                        qmark=INDENT,
                        pointer=POINTER,
                        style=custom_style_fancy,
                    ).ask()
                    if len(selection) > 0:
                        selected[package_id] = selection
                    else:
                        selected[package_id] = available[package_id].get("items", {}).keys()
                    available.pop(package_id)

            print("\n[bold]You have selected the following modules:[/] :robot:\n")

            tree = Tree("modules")
            self.build_tree(selected, tree)
            print(Padding.indent(tree, 5))
            print("\n")

            if len(available) > 0:
                if questionary.confirm("Would you like to add more?", default=False).ask():
                    continue

            loop = False
            if not questionary.confirm("Would you like to continue with creation?", default=True).ask():
                print("Exiting...")
                raise typer.Exit()
            else:
                self.create(init_dir, selected, mode)
                print("Done!")
        raise typer.Exit()


command = InteractiveInit(
    name="init", help="Initialize or upgrade a new CDF project with templates interactively."
).interactive
