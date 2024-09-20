from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path
from typing import Literal, Optional

import questionary
import typer
from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print
from rich.markdown import Markdown
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.tree import Tree

import cognite_toolkit
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import _cli_commands as CLICommands
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._changes import (
    UPDATE_MODULE_VERSION_DOCSTRING,
    AutomaticChange,
    Changes,
    ManualChange,
    UpdateModuleVersion,
)
from cognite_toolkit._cdf_tk.constants import (
    BUILTIN_MODULES,
    MODULES,
    SUPPORT_MODULE_UPGRADE_FROM_VERSION,
    EnvType,
)
from cognite_toolkit._cdf_tk.data_classes import (
    Environment,
    InitConfigYAML,
    ModuleLocation,
    ModuleResources,
    Package,
    Packages,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.hints import verify_module_directory
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


class ModulesCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False):
        super().__init__(print_warning, skip_tracking, silent)
        self._builtin_modules_path = Path(resources.files(cognite_toolkit.__name__)) / BUILTIN_MODULES  # type: ignore [arg-type]

    @staticmethod
    def _build_tree(item: Packages | ModuleLocation, tree: Tree) -> None:
        if not isinstance(item, Packages):
            return
        for key, value in item.items():
            subtree = tree.add(key)
            for subvalue in value.modules:
                subtree.add(str(subvalue))

    def _create(
        self,
        organization_dir: Path,
        selected_packages: Packages,
        environments: list[EnvType],
        mode: Literal["new", "clean", "update"] | None,
    ) -> None:
        modules_root_dir = organization_dir / MODULES
        if mode == "clean" and modules_root_dir.is_dir():
            print(f"{INDENT}[yellow]Clearing directory[/]")
            shutil.rmtree(modules_root_dir)

        modules_root_dir.mkdir(parents=True, exist_ok=True)

        seen_modules: set[Path] = set()
        selected_paths: set[Path] = set()
        for package_name, package in selected_packages.items():
            print(f"{INDENT}[{'yellow' if mode == 'clean' else 'green'}]Creating {package_name}[/]")

            for module in package.modules:
                if module.dir in seen_modules:
                    # A module can be part of multiple packages
                    continue
                seen_modules.add(module.dir)
                # Add the module and its parent paths to the selected paths, use to load the default.config.yaml
                # files
                selected_paths.update(module.parent_relative_paths)
                selected_paths.add(module.relative_path)

                print(f"{INDENT*2}[{'yellow' if mode == 'clean' else 'green'}]Creating module {module.name}[/]")
                target_dir = modules_root_dir / module.relative_path
                if Path(target_dir).exists() and mode == "update":
                    if questionary.confirm(
                        f"{INDENT}Module {module.name} already exists in folder {target_dir}. Would you like to overwrite?",
                        default=False,
                    ).ask():
                        shutil.rmtree(target_dir)
                    else:
                        continue
                shutil.copytree(module.dir, target_dir, ignore=shutil.ignore_patterns("default.*"))

        for environment in environments:
            if mode == "update":
                config_init = InitConfigYAML.load_existing(
                    (Path(organization_dir) / f"config.{environment}.yaml").read_text(), environment
                ).load_defaults(self._builtin_modules_path, selected_paths)
            else:
                config_init = InitConfigYAML(
                    Environment(
                        name=environment,
                        project=f"<my-project-{environment}>",
                        build_type=environment,
                        selected=[f"{MODULES}/"],
                    )
                ).load_defaults(self._builtin_modules_path, selected_paths)
            print(
                f"{INDENT}[{'yellow' if mode == 'clean' else 'green'}]{'Updating' if mode == 'update' else 'Creating'} config.{environment}.yaml[/]"
            )
            (Path(organization_dir) / f"config.{environment}.yaml").write_text(config_init.dump_yaml_with_comments())

        cdf_toml_content = (self._builtin_modules_path / CDFToml.file_name).read_text()
        if organization_dir != Path.cwd():
            cdf_toml_content = cdf_toml_content.replace(
                "#<PLACEHOLDER>",
                f'''
default_organization_dir = "{organization_dir.name}"''',
            )
        else:
            cdf_toml_content = cdf_toml_content.replace("#<PLACEHOLDER>", "")

        destination = Path.cwd() / CDFToml.file_name
        if destination.exists():
            print(f"{INDENT}[yellow]cdf.toml file already exists. Skipping creation.")
        else:
            destination.write_text(cdf_toml_content, encoding="utf-8")

    def init(
        self,
        organization_dir: Optional[Path] = None,
        select_all: bool = False,
        clean: bool = False,
    ) -> None:
        if not organization_dir:
            new_line = "\n    "
            message = (
                f"Which directory would you like to create templates in? (default: current directory){new_line}"
                f"HINT It is recommended to use an organization directory if you use the{new_line}repository for more than Toolkit. "
                f"If this repository is only used for Toolkit,{new_line}it is recommended to use the current directory "
                f"(assumed to be the{new_line}root of the repository):"
            )
            organization_dir_raw = questionary.text(message=message, default="").ask()
            organization_dir = Path(organization_dir_raw.strip())

        modules_root_dir = organization_dir / MODULES
        packages = Packages().load(self._builtin_modules_path)

        if select_all:
            print(Panel("Instantiating all available modules"))
            mode = self._verify_clean(modules_root_dir, clean)
            self._create(
                organization_dir=organization_dir, selected_packages=packages, environments=["dev", "prod"], mode=mode
            )
            return

        print("\n")
        print(
            Panel(
                "\n".join(
                    [
                        "Interactive process for selecting initial modules"
                        "The modules are thematically bundled in packages you can choose between. You can add more by repeating the process.",
                        "You can use the arrow keys ⬆ ⬇  on your keyboard to select modules, and press enter ⮐  to continue with your selection.",
                    ]
                ),
                title="Select initial modules",
                style="green",
                padding=(1, 2),
            )
        )
        mode = self._verify_clean(modules_root_dir, clean)

        print(f"  [{'yellow' if mode == 'clean' else 'green'}]Using directory [bold]{organization_dir}[/]")

        selected = self._select_packages(packages)

        if not questionary.confirm("Would you like to continue with creation?", default=True).ask():
            print("Exiting...")
            raise typer.Exit()

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

        self._create(organization_dir, selected, environments, mode)

        print(
            Panel(
                f"Modules have been prepared in [bold]{organization_dir}[/].",
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

    def _select_packages(self, packages: Packages, existing_module_names: list[str] | None = None) -> Packages:
        adding_to_existing = False
        if existing_module_names is not None:
            adding_to_existing = True
            for item in packages.values():
                item.modules = [module for module in item.modules if module.name not in existing_module_names]

        selected = Packages()

        while True:
            if len(selected) > 0:
                print("\n[bold]You have selected the following:[/]\n")

                tree = Tree(MODULES)
                self._build_tree(selected, tree)
                print(Padding.indent(tree, 5))
                print("\n")

                if not questionary.confirm("Would you like to make changes to the selection?", default=False).ask():
                    break

            if not any([len(package.modules) > 0 for package in packages.values()]):
                print("No more modules available.")
                break

            package: Package = questionary.select(
                "Which package would you like to use?",
                instruction="Use arrow up/down and ⮐  to save",
                choices=[
                    questionary.Choice(
                        title=f"{package.title}: {package.description} ({len(package.modules)})", value=package
                    )
                    for package in [package for package in packages.values() if len(package.modules) > 0]
                ],
                pointer=POINTER,
                style=custom_style_fancy,
            ).ask()

            if len(package.modules) > 1 or (adding_to_existing and len(package.modules) > 0):
                selection = questionary.checkbox(
                    f"Which modules in {package.name} would you like to add?",
                    instruction="Use arrow up/down, press space to select item(s) and enter to save",
                    choices=[
                        questionary.Choice(
                            title=selectable_module.title,
                            value=selectable_module,
                            checked=True,
                        )
                        for selectable_module in package.modules
                    ],
                    qmark=INDENT,
                    pointer=POINTER,
                    style=custom_style_fancy,
                ).ask()
            else:
                selection = package.modules

            selected[package.name] = Package(
                name=package.name,
                title=package.title,
                description=package.description,
                modules=selection,
            )
        return selected

    @staticmethod
    def _verify_clean(modules_root_dir: Path, clean: bool) -> Literal["new", "clean"]:
        if clean:
            return "clean"
        if not modules_root_dir.is_dir():
            return "new"
        user_selection = questionary.select(
            f"Directory {modules_root_dir} already exists. What would you like to do?",
            choices=[
                questionary.Choice("Abort", "abort"),
                questionary.Choice("Overwrite (clean existing)", "clean"),
            ],
            pointer=POINTER,
            style=custom_style_fancy,
            instruction="use arrow up/down and " + "⮐ " + " to save",
        ).ask()
        if user_selection == "abort":
            print("Aborting...")
            raise typer.Exit()
        return "clean"

    def upgrade(self, organization_dir: Path, verbose: bool = False) -> Changes:
        module_version = self._get_module_version(organization_dir)
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

        changes = Changes.load(module_version, organization_dir)
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
                        if file.is_relative_to(organization_dir):
                            print(Markdown(f"  - {file.relative_to(organization_dir).as_posix()}"))
                        else:
                            print(Markdown(f"  - {file.as_posix()}"))
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
        cdf_toml = CDFToml.load()
        # After `0.3.0` the version is in the CDF TOML file
        if cdf_toml.is_loaded_from_file and cdf_toml.modules.version:
            return parse_version(cdf_toml.modules.version)

        elif (system_yaml := project_path / "_system.yaml").exists():
            # From 0.2.0a3 we have the _system.yaml on the root of the project
            content = read_yaml_file(system_yaml)
        elif (system_yaml := project_path / "cognite_modules" / "_system.yaml").exists():
            # Up to 0.2.0a2 we have the _system.yaml in the cognite_modules folder
            content = read_yaml_file(system_yaml)
        else:
            raise ToolkitRequiredValueError(
                "No 'cdf.toml' or '_system.yaml' file found in project."
                "This is needed to determine the version of the modules."
            )
        return parse_version(content.get("cdf_toolkit_version", "0.0.0"))

    def list(self, organization_dir: Path, build_env_name: str | None) -> None:
        if organization_dir in {Path("."), Path("./")}:
            organization_dir = Path.cwd()
        verify_module_directory(organization_dir, build_env_name)
        modules = ModuleResources(organization_dir, build_env_name)

        table = Table(title=f"{build_env_name} {organization_dir.name} modules")
        table.add_column("Module Name", style="bold")
        table.add_column("Resource Folders", style="bold")
        table.add_column("Resources", style="bold")
        table.add_column("Build Warnings", style="bold")
        table.add_column("Build Result", style="bold")
        table.add_column("Location", style="bold")

        for module in modules.list():
            if module.status == "Success":
                status = f"[green]{module.status}[/]"
            else:
                status = f"[red]{module.status}[/]"
            if module.warning_count > 0:
                warning_count = f"[yellow]{module.warning_count:,}[/]"
            else:
                warning_count = f"{module.warning_count:,}"

            table.add_row(
                module.name,
                f"{len(module.resources):,}",
                f"{sum(len(resources) for resources in module.resources.values()):,}",
                warning_count,
                status,
                module.location.path.as_posix(),
            )

        print(table)

    def add(self, organization_dir: Path) -> None:
        verify_module_directory(organization_dir, "dev")

        existing_module_names = [module.name for module in ModuleResources(organization_dir, "dev").list()]
        available_packages = Packages().load(self._builtin_modules_path)

        added_packages = self._select_packages(available_packages, existing_module_names)

        environments = [env for env in EnvType.__args__ if (organization_dir / f"config.{env}.yaml").exists()]  # type: ignore[attr-defined]
        self._create(organization_dir, added_packages, environments, "update")
