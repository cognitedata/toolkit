from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Any, Optional, Union

import questionary
import typer
import yaml
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


class Packages:
    def __init__(self) -> None:
        self._packages: dict[str, dict[str, Any]] | None = None

    @property
    def content(self) -> dict[str, dict[str, Any]] | None:
        if self._packages is None:
            self._packages = self._get_packages()
        return self._packages

    def _get_packages(self) -> dict[str, dict[str, Any]] | None:
        packages = {}

        package_dir = Path(__file__).parent / ".packages"
        if not Path.exists(package_dir):
            raise FileNotFoundError(f"No packages dir found at {package_dir}")

        for root, dirs, _ in os.walk(package_dir):
            for subdir in dirs:
                yaml_file_path = os.path.join(root, subdir, "manifest.yaml")
                if os.path.exists(yaml_file_path):
                    with open(yaml_file_path) as file:
                        content = file.read()
                        packages[subdir] = yaml.CSafeLoader(content).get_data()
        return packages


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

    def create(self, init_dir: str, selected: dict[str, dict[str, Any]], mode: str | None) -> None:
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
        package: Annotated[
            Optional[str],
            typer.Option(
                help="Name of packages to include",
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
                "\n".join(
                    [
                        "Welcome to the CDF Toolkit!",
                        "This wizard will help you prepare modules in the folder you enter.",
                        "The modules are thematically bundled in packages you can choose between. You can add more by repeating the process.",
                        "You can use the arrow keys ⬆ ⬇  on your keyboard to select modules, and press enter ⮐  to continue with your selection.",
                    ]
                ),
                title="Interactive template wizard",
                style="green",
                padding=(1, 2),
            )
        )

        available = Packages().content
        if not available:
            raise ToolkitRequiredValueError("No available packages found at location")

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

        selected: dict[str, dict[str, Any]] = {}
        if package:
            selected = {package: {}}

        loop = True
        while loop:
            if len(selected) > 0:
                print("\n[bold]You have selected the following modules:[/] :robot:\n")

                tree = Tree("modules")
                self.build_tree(selected, tree)
                print(Padding.indent(tree, 5))
                print("\n")

                if len(available) > 0:
                    if not questionary.confirm("Would you like to add more?", default=False).ask():
                        loop = False
                        continue

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
                    if package_id == "none":
                        break

                selected[package_id] = {}
                selection = questionary.checkbox(
                    f"Which modules of {package_id} would you like to include?",
                    instruction="Use arrow up/down, press space to select item(s) and enter to save",
                    choices=[
                        questionary.Choice(value.get("title", key), key)
                        for key, value in available[package_id].get("modules", {}).items()
                    ],
                    qmark=INDENT,
                    pointer=POINTER,
                    style=custom_style_fancy,
                ).ask()

                if len(selection) > 0:
                    selected[package_id] = selection
                else:
                    selected[package_id] = available[package_id].get("modules", {}).keys()
                available.pop(package_id)

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
