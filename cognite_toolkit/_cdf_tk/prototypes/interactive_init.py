from __future__ import annotations

import shutil
import warnings
from collections.abc import MutableMapping
from pathlib import Path
from typing import Annotated, Any, Optional, Union

import questionary
import typer
import yaml
from rich import print
from rich.padding import Padding
from rich.panel import Panel
from rich.tree import Tree

from cognite_toolkit._cdf_tk.constants import ALT_CUSTOM_MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.prototypes import _packages

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


class Packages(dict, MutableMapping[str, dict[str, Any]]):
    @classmethod
    def load(cls) -> Packages:
        packages = {}
        for module in _packages.__all__:
            manifest = Path(_packages.__file__).parent / module / "manifest.yaml"
            if not manifest.exists():
                warnings.warn(f"Bug in Cognite-Toolkit. Manifest file not found for package {module}")
                continue
            content = manifest.read_text()
            if yaml.__with_libyaml__:
                packages[manifest.parent.name] = yaml.CSafeLoader(content).get_data()
            else:
                packages[manifest.parent.name] = yaml.SafeLoader(content).get_data()
        return cls(packages)


class InteractiveInit(typer.Typer):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.command()(self.interactive)

    def build_tree(self, item: Union[dict, list], tree: Tree) -> None:
        if not isinstance(item, dict):
            return
        for key, value in item.items():
            subtree = tree.add(key)
            for subvalue in value:
                if isinstance(subvalue, dict):
                    self.build_tree(subvalue, subtree)
                else:
                    subtree.add(subvalue)

    def create(self, init_dir_raw: str, selected: dict[str, dict[str, Any]], mode: str | None) -> None:
        init_dir = Path(init_dir_raw)
        if mode == "overwrite":
            print(f"{INDENT}[yellow]Clearing directory[/]")
            if init_dir.is_dir():
                shutil.rmtree(init_dir)

        modules_dir = init_dir / ALT_CUSTOM_MODULES
        modules_dir.mkdir(parents=True, exist_ok=True)

        includes = []

        for package_name, modules in selected.items():
            print(f"{INDENT}[{'yellow' if mode == 'overwrite' else 'green'}]Creating package {package_name}[/]")

            package_dir = modules_dir / package_name

            for module in modules:
                includes.append(package_dir)
                print(f"{INDENT*2}[{'yellow' if mode == 'overwrite' else 'green'}]Creating module {module}[/]")
                source_dir = Path(_packages.__file__).parent / package_name / module
                if not Path(source_dir).exists():
                    print(f"{INDENT*3}[red]Module {module} not found in package {package_name}. Skipping...[/]")
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
                help="Name of package to include",
            ),
        ] = None,
    ) -> None:
        """Initialize or upgrade a new CDF project with templates interactively."""

        print("\n")
        print(
            Panel(
                "\n".join(
                    [
                        "Welcome to the CDF Toolkit!",
                        "This wizard will help you prepare modules in the folder you enter.",
                        "The modules are thematically bundled in packages you can choose between. You can add more by "
                        "repeating the process.",
                        "You can use the arrow keys ⬆ ⬇  on your keyboard to select modules, and press enter ⮐  to "
                        "continue with your selection.",
                    ]
                ),
                title="Interactive template wizard",
                style="green",
                padding=(1, 2),
            )
        )

        packages = Packages.load()
        if not packages:
            raise ToolkitRequiredValueError("No available packages found at location")

        mode = "new"

        if not init_dir:
            init_dir = questionary.text(
                "Which directory would you like to create templates in? (typically customer name)",
                default="new_project",
            ).ask()

        if init_dir and Path(init_dir).is_dir():
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

        if not init_dir:
            raise ToolkitRequiredValueError("Directory path is required.")

        print(f"  [{'yellow' if mode == 'overwrite' else 'green'}]Using directory [bold]{init_dir}[/]")

        selected: dict[str, dict[str, Any]] = {}
        if package:
            if not packages.get(package):
                raise ToolkitRequiredValueError(
                    f"Package {package} is not known. Available packages are {', '.join(packages.keys())}"
                )

            selected[package] = packages[package].get("modules", {}).keys()

        loop = True
        while loop:
            if len(selected) > 0:
                print("\n[bold]You have selected the following modules:[/] :robot:\n")

                tree = Tree("modules")
                self.build_tree(selected, tree)
                print(Padding.indent(tree, 5))
                print("\n")

                if len(packages) > 0:
                    if not questionary.confirm("Would you like to add more?", default=False).ask():
                        loop = False
                        continue

                package_id = questionary.select(
                    "Which package would you like to include?",
                    instruction="Use arrow up/down and ⮐  to save",
                    choices=[questionary.Choice(value.get("title", key), key) for key, value in packages.items()],
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
                        for key, value in packages[package_id].get("modules", {}).items()
                    ],
                    qmark=INDENT,
                    pointer=POINTER,
                    style=custom_style_fancy,
                ).ask()

                if len(selection) > 0:
                    selected[package_id] = selection
                else:
                    selected[package_id] = packages[package_id].get("modules", {}).keys()
                packages.pop(package_id)

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
