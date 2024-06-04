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


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def get_packages(self) -> dict[str, dict[str, Any]]:
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
                help="List of modules to include. Options are 'get_content()'",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        print(Panel("Initializing or upgrading a new CDF project with templates interactively."))

        selected: dict[str, Any] = {}
        available = self.get_packages()
        mode = None

        if not init_dir:
            init_dir = questionary.text("Which directory would you like to use?", default="new_project").ask()

        if init_dir and Path(init_dir).is_dir():
            mode = questionary.select(
                "Directory already exists. What would you like to do?",
                choices=[
                    questionary.Choice("Abort", "abort"),
                    questionary.Choice("Overwrite", "overwrite"),
                    questionary.Choice("Update", "update"),
                ],
            ).ask()
            if mode == "abort":
                print("Aborting...")
                raise typer.Exit()

        if not init_dir:
            raise ToolkitRequiredValueError("Directory path is required.")

        if not mode:
            mode = "new"
        print(f"  [{'yellow' if mode == 'overwrite' else 'green'}]Using directory [bold]{init_dir}[/]")

        loop = True
        while loop:
            if not arg_selected:
                package_id = questionary.select(
                    "Which package would you like to include?",
                    instruction="Use arrow up/down and enter to select",
                    choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                ).ask()

                if package_id:
                    selected[package_id] = []
                    selection = questionary.checkbox(
                        f"Which modules of {package_id} would you like to include?",
                        instruction="Use arrow up/down, space to select and enter to save",
                        choices=[
                            questionary.Choice(value.get("title", key), key)
                            for key, value in available[package_id].get("items", {}).items()
                        ],
                        qmark=INDENT,
                    ).ask()
                    selected[package_id] = selection
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
