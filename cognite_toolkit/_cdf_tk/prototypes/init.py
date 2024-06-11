from __future__ import annotations

import shutil
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Optional

import questionary
import typer
import yaml
from rich import print
from rich.padding import Padding
from rich.panel import Panel
from rich.tree import Tree

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.prototypes import _packages

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

INDENT = "  "
POINTER = INDENT + "▶"


class Packages(dict, MutableMapping[str, dict[str, Any]]):
    @classmethod
    def load(cls) -> Packages:
        packages = {}
        for module in _packages.__all__:
            manifest = Path(_packages.__file__).parent / module / "manifest.yaml"
            if not manifest.exists():
                continue
            content = manifest.read_text()
            if yaml.__with_libyaml__:
                packages[manifest.parent.name] = yaml.CSafeLoader(content).get_data()
            else:
                packages[manifest.parent.name] = yaml.SafeLoader(content).get_data()
        return cls(packages)


class InitCommand(ToolkitCommand):
    def _build_tree(self, item: dict | list, tree: Tree) -> None:
        if isinstance(item, dict):
            for key, value in item.items():
                subtree = tree.add(key)
                for subvalue in value:
                    if isinstance(subvalue, dict):
                        self._build_tree(subvalue, subtree)
                    else:
                        subtree.add(subvalue)

    def _create(self, init_dir: str, selected: dict[str, dict[str, Any]], mode: str | None) -> None:
        if mode == "overwrite":
            print(f"{INDENT}[yellow]Clearing directory[/]")
            if Path.is_dir(Path(init_dir)):
                shutil.rmtree(init_dir)

        modules_dir = Path(init_dir) / "modules"
        modules_dir.mkdir(parents=True, exist_ok=True)

        includes = []

        for package, modules in selected.items():
            print(f"{INDENT}[{'yellow' if mode == 'overwrite' else 'green'}]Creating {package}[/]")

            package_dir = modules_dir / package

            for module in modules:
                includes.append(package_dir)
                print(f"{INDENT*2}[{'yellow' if mode == 'overwrite' else 'green'}]Creating module {module}[/]")
                source_dir = Path(_packages.__file__).parent / package / module
                if not Path(source_dir).exists():
                    print(f"{INDENT*3}[red]Module {module} not found in package {package}. Skipping...[/]")
                    continue
                module_dir = package_dir / module
                if Path(module_dir).exists() and mode == "update":
                    if questionary.confirm(
                        f"{INDENT}Module {module} already exists in folder {package_dir}. Would you like to overwrite?",
                        default=False,
                    ).ask():
                        shutil.rmtree(module_dir)
                    else:
                        continue

                shutil.copytree(source_dir, module_dir)

    def run(self, ctx: typer.Context, init_dir: Optional[str] = None, arg_package: Optional[str] = None) -> None:
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

        available = Packages().load()
        if not available:
            raise ToolkitRequiredValueError("No available packages found at location")

        mode = "new"

        if not init_dir:
            init_dir = questionary.text(
                "Which directory would you like to create templates in? (typically customer name)",
                default="new_project",
            ).ask()
            if not init_dir or init_dir.strip() == "":
                raise ToolkitRequiredValueError("You must provide a directory name.")
        else:
            if Path(init_dir).is_dir():
                mode = questionary.select(
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

        print(f"  [{'yellow' if mode == 'overwrite' else 'green'}]Using directory [bold]{init_dir}[/]")

        selected: dict[str, dict[str, Any]] = {}
        if arg_package:
            if not available.get(arg_package):
                raise ToolkitRequiredValueError(
                    f"Package {arg_package} is unknown. Available packages are {', '.join(available)}"
                )
            else:
                selected[arg_package] = available[arg_package].get("modules", {}).keys()
                available.pop(arg_package)

        while True:
            if len(selected) > 0:
                # special case for no packages, i.e. the user wants to skip selection
                if "empty" in selected:
                    break

                print("\n[bold]You have selected the following modules:[/]\n")

                tree = Tree("modules")
                self._build_tree(selected, tree)
                print(Padding.indent(tree, 5))
                print("\n")

                if len(available) > 0:
                    if not questionary.confirm("Would you like to add more?", default=False).ask():
                        break

            package_id = questionary.select(
                "Which package would you like to include?",
                instruction="Use arrow up/down and ⮐  to save",
                choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                pointer=POINTER,
                style=custom_style_fancy,
            ).ask()

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

        if not questionary.confirm("Would you like to continue with creation?", default=True).ask():
            print("Exiting...")
            raise typer.Exit()
        else:
            self._create(init_dir, selected, mode)
            print("Done!")
        raise typer.Exit()
