from __future__ import annotations

import shutil
import subprocess
from collections.abc import MutableMapping
from contextlib import suppress
from importlib import resources
from pathlib import Path
from typing import Any, Optional

import questionary
import typer
import yaml
from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print
from rich.markdown import Markdown
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.tree import Tree

import cognite_toolkit
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import ALT_CUSTOM_MODULES, COGNITE_MODULES, SUPPORT_MODULE_UPGRADE_FROM_VERSION
from cognite_toolkit._cdf_tk.data_classes import Environment, InitConfigYAML, SystemYAML
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.prototypes import _packages
from cognite_toolkit._cdf_tk.prototypes.commands._changes import (
    UPDATE_MODULE_VERSION_DOCSTRING,
    AutomaticChange,
    Changes,
    ManualChange,
    UpdateModuleVersion,
)
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_file
from cognite_toolkit._version import __version__

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


class ModulesCommand(ToolkitCommand):
    def _build_tree(self, item: dict | list, tree: Tree) -> None:
        if not isinstance(item, dict):
            return
        for key, value in item.items():
            subtree = tree.add(key)
            for subvalue in value:
                if isinstance(subvalue, dict):
                    self._build_tree(subvalue, subtree)
                else:
                    subtree.add(subvalue)

    def _create(
        self, init_dir: str, selected: dict[str, dict[str, Any]], environments: list[str], mode: str | None
    ) -> None:
        modules_root_dir = Path(init_dir) / ALT_CUSTOM_MODULES
        if mode == "overwrite":
            if modules_root_dir.is_dir():
                print(f"{INDENT}[yellow]Clearing directory[/]")
                shutil.rmtree(modules_root_dir)

        modules_root_dir.mkdir(parents=True, exist_ok=True)

        for package, modules in selected.items():
            print(f"{INDENT}[{'yellow' if mode == 'overwrite' else 'green'}]Creating {package}[/]")

            for module in modules:
                print(f"{INDENT*2}[{'yellow' if mode == 'overwrite' else 'green'}]Creating module {module}[/]")
                source_dir = Path(_packages.__file__).parent / package / module
                if not Path(source_dir).exists():
                    print(f"{INDENT*3}[red]Module {module} not found in package {package}. Skipping...[/]")
                    continue
                module_dir = modules_root_dir / package / module
                if Path(module_dir).exists() and mode == "update":
                    if questionary.confirm(
                        f"{INDENT}Module {module} already exists in folder {module_dir}. Would you like to overwrite?",
                        default=False,
                    ).ask():
                        shutil.rmtree(module_dir)
                    else:
                        continue

                shutil.copytree(source_dir, module_dir, ignore=shutil.ignore_patterns("default.*"))

        for environment in environments:
            # if mode == "update":
            config_init = InitConfigYAML(
                Environment(
                    name=environment,
                    project=f"<my-project-{environment}>",
                    build_type="dev" if environment == "dev" else "prod",
                    selected=list(selected.keys()) if selected else ["empty"],
                )
            ).load_selected_defaults(Path(_packages.__file__).parent)
            print(f"{INDENT}[{'yellow' if mode == 'overwrite' else 'green'}]Creating config.{environment}.yaml[/]")
            Path(init_dir + f"/config.{environment}.yaml").write_text(config_init.dump_yaml_with_comments())

        _system_yaml_file = Path(resources.files(cognite_toolkit.__name__)) / SystemYAML.file_name  # type: ignore[arg-type]
        _system_yaml = read_yaml_file(_system_yaml_file)
        _system_yaml.pop("packages", None)

        (Path(init_dir) / SystemYAML.file_name).write_text(f"#DO NOT EDIT THIS FILE!\n{yaml.safe_dump(_system_yaml)}")

    def init(self, init_dir: Optional[str] = None, arg_package: Optional[str] = None) -> None:
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

        modules_root_dir = Path(init_dir) / ALT_CUSTOM_MODULES
        if modules_root_dir.is_dir():
            mode = questionary.select(
                f"Directory {modules_root_dir} already exists. What would you like to do?",
                choices=[
                    questionary.Choice("Abort", "abort"),
                    questionary.Choice("Overwrite (clean existing)", "overwrite"),
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
                print("\n[bold]You have selected the following modules:[/]\n")

                tree = Tree(ALT_CUSTOM_MODULES)
                self._build_tree(selected, tree)
                print(Padding.indent(tree, 5))
                print("\n")

                if len(available) > 0:
                    if not questionary.confirm("Would you like to make changes to the selection?", default=False).ask():
                        break

            package_id = questionary.select(
                "Which package would you like to include?",
                instruction="Use arrow up/down and ⮐  to save",
                choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                pointer=POINTER,
                style=custom_style_fancy,
            ).ask()

            if len(available[package_id].get("modules", {}).items()) > 1:
                selection = questionary.checkbox(
                    f"Which modules in {package_id} would you like to include?",
                    instruction="Use arrow up/down, press space to select item(s) and enter to save",
                    choices=[
                        questionary.Choice(
                            value.get("title", key), key, checked=True if key in selected.get(package_id, {}) else False
                        )
                        for key, value in available[package_id].get("modules", {}).items()
                    ],
                    qmark=INDENT,
                    pointer=POINTER,
                    style=custom_style_fancy,
                ).ask()
            else:
                selection = list(available[package_id].get("modules", {}).keys())

            selected[package_id] = selection

        if not questionary.confirm("Would you like to continue with creation?", default=True).ask():
            print("Exiting...")
            raise typer.Exit()
        else:
            environments = questionary.checkbox(
                "Which environments would you like to include?",
                instruction="Use arrow up/down, press space to select item(s) and enter to save",
                choices=[
                    questionary.Choice(title="dev", checked=True),
                    questionary.Choice(title="prod", checked=True),
                    questionary.Choice(title="staging", checked=False),
                ],
                qmark=INDENT,
                pointer=POINTER,
                style=custom_style_fancy,
            ).ask()
            self._create(init_dir, selected, environments, mode)
            print(
                Panel(
                    f"""Modules have been prepared in [bold]{init_dir}[/]. \nNext steps:
    1. Run `cdf-tk auth verify --interactive to set up credentials.
    2. Configure your project in the config files. Use cdf-tk build for assistance.
    3. Run `cdf-tk deploy --dry-run` to verify the deployment.""",
                    style="green",
                )
            )

            if "empty" in selected:
                print(
                    Panel(
                        "Please check out https://developer.cognite.com/sdks/toolkit/modules/ for guidance on writing custom modules",
                    )
                )

        raise typer.Exit()

    def upgrade(self, project_dir: str | Path | None = None, verbose: bool = False) -> Changes:
        project_path = Path(project_dir or ".")
        module_version = self._get_module_version(project_path)
        cli_version = parse_version(__version__)

        if cli_version < module_version:
            upgrade = "poetry add cognite-toolkit@" if CLICommands.use_poetry() else "pip install cognite-toolkit=="
            print(
                f"Modules are at a higher version ({module_version}) than the installed CLI ({__version__})."
                f"Please upgrade the CLI to match the modules: `{upgrade}{module_version}`."
            )
            return Changes()

        if module_version < Version(SUPPORT_MODULE_UPGRADE_FROM_VERSION):
            print(
                f"The modules upgrade command is not supported for versions below {SUPPORT_MODULE_UPGRADE_FROM_VERSION}."
            )
            return Changes()

        if not CLICommands.use_git():
            self.warn(MediumSeverityWarning("git is not installed. It is strongly recommended to use version control."))
        else:
            if not CLICommands.has_initiated_repo():
                self.warn(MediumSeverityWarning("git repository not initiated. Did you forget to run `git init`?"))
            else:
                if CLICommands.has_uncommitted_changes():
                    print("Uncommitted changes detected. Please commit your changes before upgrading the modules.")
                    return Changes()
        # Update the docstring of the change 'UpdateModuleVersion' to be more informative
        UpdateModuleVersion.__doc__ = UPDATE_MODULE_VERSION_DOCSTRING.format(
            module_version=module_version, cli_version=cli_version
        )

        changes = Changes.load(module_version, project_path)
        if not changes:
            print("No changes required.")
            return changes

        print(
            Panel(
                f"Found {len(changes)} changes from {module_version} to {cli_version}",
                title="Upgrade Modules",
                style="green",
            )
        )

        total_changed: set[Path] = set()
        for change in changes:
            color = "green"
            changed_files: set[Path] = set()
            if change.has_file_changes:
                if isinstance(change, AutomaticChange):
                    changed_files = change.do()
                    color = "yellow" if changed_files else "green"
                    total_changed.update(changed_files)
                elif isinstance(change, ManualChange):
                    changed_files = change.needs_to_change()
                    color = "red" if changed_files else "green"
            print(
                Panel(
                    f"Change: {type(change).__name__}",
                    style=color,
                    expand=False,
                )
            )
            if not changed_files and change.has_file_changes:
                suffix = "have been changed" if isinstance(change, AutomaticChange) else "need to be changed"
                print(f"No files {suffix}.")
            else:
                if isinstance(change, ManualChange):
                    print(Markdown(change.instructions(changed_files)))
                elif isinstance(change, AutomaticChange):
                    print("The following files have been changed:")
                    for file in changed_files:
                        print(Markdown(f"  - {file.relative_to(project_path).as_posix()}"))
            if changed_files or not change.has_file_changes or verbose:
                print(Markdown(change.__doc__ or "Missing description."))
            print(Rule())

        use_git = CLICommands.use_git() and CLICommands.has_initiated_repo()
        summary = ["All automatic changes have been applied."]
        color = "green"
        if total_changed:
            summary.append(f"A total of {len(total_changed)} files have been changed.")
        else:
            summary.append("No files have been changed.")
        if manual_changes := [change for change in changes if isinstance(change, ManualChange)]:
            summary.append(
                f"A total of {len(manual_changes)} changes require manual intervention: {', '.join([type(change).__name__ for change in manual_changes])}."
            )
            color = "yellow"
        if use_git and total_changed:
            summary.append("\nPlease review the changes and commit them if you are satisfied.")
            summary.append("You can use `git diff` to see the changes or use your IDE to inspect the changes.")
            summary.append(
                "If you are not satisfied with the changes, you can use `git checkout -- <file>` to revert "
                "a file or `git checkout .` to revert all changes."
            )
        print(Panel("\n".join(summary), title="Upgrade Complete", style=color))
        return changes

    @staticmethod
    def _get_module_version(project_path: Path) -> Version:
        if (system_yaml := project_path / SystemYAML.file_name).exists():
            # From 0.2.0a3 we have the _system.yaml on the root of the project
            content = read_yaml_file(system_yaml)
        elif (system_yaml := project_path / COGNITE_MODULES / SystemYAML.file_name).exists():
            # Up to 0.2.0a2 we have the _system.yaml in the cognite_modules folder
            content = read_yaml_file(system_yaml)
        else:
            raise ToolkitRequiredValueError("No system.yaml file found in project.")
        return parse_version(content.get("cdf_toolkit_version", "0.0.0"))


class CLICommands:
    @classmethod
    def use_poetry(cls) -> bool:
        with suppress(Exception):
            return shutil.which("poetry") is not None
        return False

    @classmethod
    def use_git(cls) -> bool:
        with suppress(Exception):
            return shutil.which("git") is not None
        return False

    @classmethod
    def has_initiated_repo(cls) -> bool:
        with suppress(Exception):
            result = subprocess.run("git rev-parse --is-inside-work-tree".split(), stdout=subprocess.PIPE)
            return result.returncode == 0
        return False

    @classmethod
    def has_uncommitted_changes(cls) -> bool:
        with suppress(Exception):
            result = subprocess.run("git diff --quiet".split(), stdout=subprocess.PIPE)
            return result.returncode != 0
        return False
