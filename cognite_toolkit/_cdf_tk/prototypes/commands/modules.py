from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterator, MutableMapping, MutableSequence
from contextlib import suppress
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
from rich.tree import Tree

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import ALT_CUSTOM_MODULES, COGNITE_MODULES, SUPPORT_MODULE_UPGRADE_FROM_VERSION
from cognite_toolkit._cdf_tk.data_classes import Environment, InitConfigYAML, SystemYAML
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.prototypes import _packages
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
        if mode == "overwrite":
            print(f"{INDENT}[yellow]Clearing directory[/]")
            if Path.is_dir(Path(init_dir)):
                shutil.rmtree(init_dir)

        modules_root_dir = Path(init_dir) / ALT_CUSTOM_MODULES
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

        if (Path(init_dir) / ALT_CUSTOM_MODULES).is_dir():
            mode = questionary.select(
                f"Directory {init_dir}/modules already exists. What would you like to do?",
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
                    if not questionary.confirm("Would you like to add more?", default=False).ask():
                        break

            package_id = questionary.select(
                "Which package would you like to include?",
                instruction="Use arrow up/down and ⮐  to save",
                choices=[questionary.Choice(value.get("title", key), key) for key, value in available.items()],
                pointer=POINTER,
                style=custom_style_fancy,
            ).ask()

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

            if len(selection) > 0:
                selected[package_id] = selection
            else:
                selected[package_id] = available[package_id].get("modules", {}).keys()

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

    def upgrade(self, project_dir: str | Path | None = None) -> None:
        project_path = Path(project_dir or ".")
        module_version = self._get_module_version(project_path)
        cli_version = parse_version(__version__)

        if cli_version < module_version:
            upgrade = "poetry add cognite-toolkit@" if CLICommands.use_poetry() else "pip install cognite-toolkit=="
            print(
                f"Modules are at a higher version ({module_version}) than the installed CLI ({__version__})."
                f"Please upgrade the CLI to match the modules: `{upgrade}{module_version}`."
            )
            return

        if module_version < Version(SUPPORT_MODULE_UPGRADE_FROM_VERSION):
            print(
                f"The modules upgrade command is not supported for versions below {SUPPORT_MODULE_UPGRADE_FROM_VERSION}."
            )
            return

        if not CLICommands.use_git():
            self.warn(MediumSeverityWarning("git is not installed. It is strongly recommended to use version control."))
        else:
            if not CLICommands.has_initiated_repo():
                self.warn(MediumSeverityWarning("git repository not initiated. Did you forget to run `git init`?"))
            else:
                if CLICommands.has_uncommitted_changes():
                    print("Uncommitted changes detected. Please commit your changes before upgrading the modules.")
                    return
        # Update the docstring of the change 'UpdateModuleVersion' to be more informative
        UpdateModuleVersion.__doc__ = (UpdateModuleVersion.__doc__ or "").format(
            module_version=module_version, cli_version=cli_version
        )

        changes = Changes.load(module_version, project_path)
        if not changes:
            print("No changes required.")
            return

        print(
            Panel(
                f"Found {len(changes)} changes from {module_version} to {cli_version}",
                title="Upgrade Modules",
                style="green",
            )
        )

        total_changed: set[Path] = set()
        for change in changes:
            print(Markdown(change.__doc__ or type(change).__name__))
            if change.has_file_changes:
                changed_files = change.do()
                if changed_files:
                    total_changed.update(changed_files)
                    print(f"Changed files: {', '.join(file.as_posix() for file in changed_files)}")
                else:
                    print("No files changed.")

        use_git = CLICommands.use_git() and CLICommands.has_initiated_repo()
        summary = ["All changes have been applied."]
        if total_changed:
            summary.append(f"A total of {len(total_changed)} files have been changed.")
        else:
            summary.append("No files have been changed.")
        if use_git and total_changed:
            summary.append("Please review the changes and commit them if you are satisfied.")
            summary.append("You can use `git diff` to see the changes or use your IDE to inspect the changes.")
            summary.append(
                "If you are not satisfied with the changes, you can use `git checkout -- <file>` to revert "
                "a file or `git checkout .` to revert all changes."
            )
        print(Panel("\n".join(summary), title="Upgrade Complete", style="green"))

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


class Change:
    """A change is a single migration step that can be applied to a project."""

    deprecated_from: Version
    required_from: Version | None = None
    has_file_changes: bool = False

    def __init__(self, project_dir: Path) -> None:
        self._project_path = project_dir

    def do(self) -> set[Path]:
        return set()


class SystemYAMLMoved(Change):
    """The _system.yaml file is now expected to in the root of the project.
    Before it was expected to be in the cognite_modules folder.
    This change moves the file to the root of the project.

    For example:
    Before:
        my_project/
            cognite_modules/
                _system.yaml
    After:
        my_project/
            _system.yaml
    """

    deprecated_from = Version("0.2.0a3")
    required_from = Version("0.2.0a3")
    has_file_changes = True

    def do(self) -> set[Path]:
        system_yaml = self._project_path / COGNITE_MODULES / SystemYAML.file_name
        if not system_yaml.exists():
            return set()
        new_system_yaml = self._project_path / SystemYAML.file_name
        system_yaml.rename(new_system_yaml)
        return {system_yaml}


class RenamedModulesSection(Change):
    """The 'modules' section in the config files has been renamed to 'variables'.
    This change updates the config files to use the new name.

    For example in config.dev.yaml:
    Before:
        ```yaml
            variables:
              cognite_modules:
                cdf_cluster: ${CDF_CLUSTER}
                cicd_clientId: ${IDP_CLIENT_ID}
                cicd_clientSecret: ${IDP_CLIENT_SECRET}
        ```
    After:
        ```yaml
            variables:
              cognite_modules:
                cdf_cluster: ${CDF_CLUSTER}
                cicd_clientId: ${IDP_CLIENT_ID}
                cicd_clientSecret: ${IDP_CLIENT_SECRET}
        ```
    """

    deprecated_from = Version("0.2.0a3")
    required_from = Version("0.2.0a3")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed: set[Path] = set()
        for config_yaml in self._project_path.glob("config.*.yaml"):
            data_raw = config_yaml.read_text()
            # We do not parse the YAML file to avoid removing comments
            updated_file: list[str] = []
            for line in data_raw.splitlines():
                if line.startswith("modules:"):
                    changed.add(config_yaml)
                    updated_file.append(line.replace("modules:", "variables:"))
                else:
                    updated_file.append(line)
            config_yaml.write_text("\n".join(updated_file))
        return changed


class BuildCleanFlag(Change):
    """The `cdf-tk build` command no longer accepts the `--clean` flag.

    The build command now always cleans the build directory before building.
    To avoid cleaning the build directory, you can use the `--no-clean` flag.
    """

    deprecated_from = Version("0.2.0a3")
    required_from = Version("0.2.0a3")
    has_file_changes = False


class CommonFunctionCodeNotSupported(Change):
    """Cognite-Toolkit no longer supports the common functions code."""

    deprecated_from = Version("0.2.0a4")
    required_from = Version("0.2.0a4")
    has_file_changes = True

    def do(self) -> set[Path]:
        # It is complex to move the common functions code, so we will just remove
        # the one module that uses it
        # Todo implement this
        cdf_functions_dummy = self._project_path / "cognite_modules" / "examples" / "cdf_functions_dummy"

        if not cdf_functions_dummy.exists():
            return set()
        shutil.rmtree(cdf_functions_dummy)
        return {cdf_functions_dummy}


class FunctionExternalDataSetIdRenamed(Change):
    """The 'externalDataSetId' field in function YAML files has been renamed to 'dataSetExternalId'.
    This change updates the function YAML files to use the new name.

    The motivation for this change is to make the naming consistent with the rest of the Toolkit.

    For example, in functions/my_function.yaml:

    Before:
        ```yaml
        externalDataSetId: my_external_id
        ```
    After:
        ```yaml
        dataSetExternalId: my_external_id
        ```
    """

    deprecated_from = Version("0.2.0a5")
    required_from = Version("0.2.0a5")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed: set[Path] = set()
        for resource_yaml in self._project_path.glob("*.yaml"):
            if resource_yaml.parent == "functions":
                content = resource_yaml.read_text()
                if "externalDataSetId" in content:
                    changed.add(resource_yaml)
                    content = content.replace("externalDataSetId", "dataSetExternalId")
                    resource_yaml.write_text(content)
        return changed


class ConfigYAMLSelectedRenaming(Change):
    """The 'environment.selected_modules_and_packages' field in the config.yaml files has been
    renamed to 'selected'.
    This change updates the config files to use the new name.

    For example, in config.dev.yaml:

    Before:
        ```yaml
        environment:
          selected_modules_and_packages:
            - my_module
        ```
    After:
        ```yaml
        environment:
          selected:
            - my_module
        ```
    """

    deprecated_from = Version("0.2.0b1")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for config_yaml in self._project_path.glob("config.*.yaml"):
            data = config_yaml.read_text()
            if "selected_modules_and_packages" in data:
                changed.add(config_yaml)
                data = data.replace("selected_modules_and_packages", "selected")
                config_yaml.write_text(data)
        return changed


class RequiredFunctionLocation(Change):
    """Function Resource YAML files are now expected to be in the 'functions' folder.
    Before they could be in subfolders inside the 'functions' folder.

    This change moves the function YAML files to the 'functions' folder.

    For example:
    Before:
        modules/
          my_module/
              functions/
                some_subdirectory/
                    my_function.yaml
    After:
        modules/
          my_module/
              functions/
                my_function.yaml
    """

    deprecated_from = Version("0.2.0b3")
    required_from = Version("0.2.0b3")
    has_file_changes = True

    def do(self) -> set[Path]:
        changed = set()
        for resource_yaml in self._project_path.glob("functions/**/*.yaml"):
            if self._is_function(resource_yaml):
                new_path = self._new_path(resource_yaml)
                if new_path != resource_yaml:
                    resource_yaml.rename(new_path)
                    changed.add(new_path)
        return changed

    @staticmethod
    def _is_function(resource_yaml: Path) -> bool:
        # Functions require a 'name' field and to distinguish from a FunctionSchedule
        # we check that the 'cronExpression' field is not present
        parsed = read_yaml_file(resource_yaml)
        if isinstance(parsed, dict):
            return "name" in parsed and "cronExpression" not in parsed
        elif isinstance(parsed, list):
            return all("name" in item and "cronExpression" not in item for item in parsed)
        return False

    @staticmethod
    def _new_path(resource_yaml: Path) -> Path:
        # Search the path for the 'functions' folder and move the file there
        for parent in resource_yaml.parents:
            if parent.name == "functions":
                return parent / resource_yaml.name
        return resource_yaml


class UpdateModuleVersion(Change):
    """In the _system.yaml file, the 'cdf_toolkit_version' field has been updated to the same version as the CLI.

    This change updates the 'cdf_toolkit_version' field in the _system.yaml file to the same version as the CLI.

    For example, in _system.yaml:
    Before:
        ```yaml
        cdf_toolkit_version: {module_version}
        ```
    After:
        ```yaml
        cdf_toolkit_version: {cli_version}
        ```
    """

    deprecated_from = parse_version(__version__)
    required_from = parse_version(__version__)
    has_file_changes = True

    def do(self) -> set[Path]:
        system_yaml = self._project_path / SystemYAML.file_name
        if not system_yaml.exists():
            return set()
        raw = system_yaml.read_text()
        new_system_yaml = []
        changes: set[Path] = set()
        # We do not parse the YAML file to avoid removing comments
        for line in raw.splitlines():
            if line.startswith("cdf_toolkit_version:"):
                new_system_yaml.append(f"cdf_toolkit_version: {__version__}")
                changes.add(system_yaml)
            else:
                new_system_yaml.append(line)
        system_yaml.write_text("\n".join(new_system_yaml))
        return changes


_CHANGES: list[type[Change]] = [change for change in Change.__subclasses__()]


class Changes(list, MutableSequence[Change]):
    @classmethod
    def load(cls, module_version: Version, project_path: Path) -> Changes:
        return cls([change(project_path) for change in _CHANGES if change.deprecated_from >= module_version])

    @property
    def required_changes(self) -> Changes:
        return Changes([change for change in self if change.required_from is not None])

    @property
    def optional_changes(self) -> Changes:
        return Changes([change for change in self if change.required_from is None])

    def __iter__(self) -> Iterator[Change]:
        return super().__iter__()
