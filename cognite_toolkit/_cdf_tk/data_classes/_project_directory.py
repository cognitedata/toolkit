from __future__ import annotations

import shutil
import tempfile
import urllib
import zipfile
from abc import abstractmethod
from contextlib import suppress
from importlib import resources
from pathlib import Path
from typing import ClassVar

from packaging.version import Version
from packaging.version import parse as parse_version
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES, ROOT_MODULES
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
    ToolkitFileNotFoundError,
    ToolkitIsADirectoryError,
    ToolkitMigrationError,
    ToolkitModuleVersionError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, iterate_modules, read_yaml_file
from cognite_toolkit._version import __version__ as current_version

from ._config_yaml import ConfigYAMLs
from ._migration_yaml import MigrationYAML


class ProjectDirectory:
    """This represents the project directory, and is used in the init command.

    It is responsible for copying the files from the templates to the project directory.

    Args:
        project_dir: The project directory.
        dry_run: Whether to do a dry run or not.
    """

    _files_to_copy: ClassVar[list[str]] = [
        "README.md",
        ".gitignore",
        ".env.tmpl",
        "_system.yaml",
    ]
    _directories_to_copy: ClassVar[list[str]] = []

    def __init__(self, project_dir: Path, dry_run: bool):
        self.project_dir = project_dir
        self._dry_run = dry_run
        self._source = Path(resources.files("cognite_toolkit"))  # type: ignore[arg-type]
        self.modules_by_root: dict[str, list[str]] = {}
        for root_module in ROOT_MODULES:
            if not (self._source / root_module).exists():
                continue
            self.modules_by_root[root_module] = [
                f"{module.relative_to(self._source)!s}" for module, _ in iterate_modules(self._source / root_module)
            ]

    def set_source(self, git_branch: str | None) -> None: ...

    @property
    def target_dir_display(self) -> str:
        return f"'{self.project_dir.relative_to(Path.cwd())!s}'"

    @abstractmethod
    def create_project_directory(self, clean: bool) -> None: ...

    def print_what_to_copy(self) -> None:
        copy_prefix = "Would" if self._dry_run else "Will"
        print(f"{copy_prefix} copy these files to {self.target_dir_display}:")
        print(self._files_to_copy)

        for root_module, modules in self.modules_by_root.items():
            print(f"{copy_prefix} copy these modules to {self.target_dir_display} from {root_module}:")
            print(modules)

    def copy(self, verbose: bool) -> None:
        dry_run = self._dry_run
        copy_prefix = "Would copy" if dry_run else "Copying"
        for filename in self._files_to_copy:
            if verbose:
                print(f"{copy_prefix} file {filename} to {self.target_dir_display}")
            if not dry_run:
                if filename == "README.md":
                    content = (self._source / filename).read_text().replace("<MY_PROJECT>", self._source.name)
                    (self.project_dir / filename).write_text(content)
                else:
                    shutil.copyfile(self._source / filename, self.project_dir / filename)

        for directory in self._directories_to_copy:
            if verbose:
                print(f"{copy_prefix} directory {directory} to {self.target_dir_display}")
            if not dry_run:
                shutil.copytree(self._source / directory, self.project_dir / directory, dirs_exist_ok=True)

        for root_module, modules in self.modules_by_root.items():
            if verbose:
                print(f"{copy_prefix} the following modules from  {root_module} to {self.target_dir_display}")
                print(modules)
            if not dry_run:
                (Path(self.project_dir) / root_module).mkdir(exist_ok=True)
                # Default files are not copied, as they are only used to setup the config.yaml.
                shutil.copytree(
                    self._source / root_module,
                    self.project_dir / root_module,
                    dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("default.*"),
                )

    def upsert_config_yamls(self, clean: bool) -> None:
        # Creating the config.[environment].yaml files
        environment_default = self._source / COGNITE_MODULES / "default.environments.yaml"
        if not environment_default.is_file():
            location = environment_default.parent.relative_to(Path.cwd())
            raise ToolkitFileNotFoundError(
                f"Could not find default.environments.yaml in {location!s}. There is something wrong with your "
                "installation, try to reinstall `cognite-tk`, and if the problem persists, please contact support."
            )

        config_yamls = ConfigYAMLs.load_default_environments(read_yaml_file(environment_default))

        config_yamls.load_default_variables(self._source)
        config_yamls.load_variables(self._source)

        for environment, config_yaml in config_yamls.items():
            config_filepath = self.project_dir / f"config.{environment}.yaml"

            print(f"Created config for {environment!r} environment.")
            if self._dry_run:
                print(f"Would write {config_filepath.name!r} to {self.target_dir_display}")
            else:
                config_filepath.write_text(config_yaml.dump_yaml_with_comments(indent_size=2))
                print(f"Wrote {config_filepath.name!r} file to {self.target_dir_display}")

    @abstractmethod
    def done_message(self) -> str:
        raise NotImplementedError()


class ProjectDirectoryInit(ProjectDirectory):
    """This represents the project directory, and is used in the init command.
    It is used when creating a new project (or overwriting an existing one).
    """

    def create_project_directory(self, clean: bool) -> None:
        if self.project_dir.exists() and not clean:
            raise ToolkitIsADirectoryError(f"Directory {self.target_dir_display} already exists.")
        elif self.project_dir.exists() and clean and self._dry_run:
            print(f"Would clean out directory {self.target_dir_display}...")
        elif self.project_dir.exists() and clean:
            print(f"Cleaning out directory {self.target_dir_display}...")
            shutil.rmtree(self.project_dir)

        if not self._dry_run:
            self.project_dir.mkdir(exist_ok=True)

    def done_message(self) -> str:
        return f"A new project was created in {self.target_dir_display}."


class ProjectDirectoryUpgrade(ProjectDirectory):
    """This represents the project directory, and is used in the init command.

    It is used when upgrading an existing project.

    """

    def __init__(self, project_dir: Path, dry_run: bool):
        super().__init__(project_dir, dry_run)
        if not project_dir.exists():
            # Need to do this check here, as we load version from the project directory.
            raise ToolkitNotADirectoryError(f"Found no directory {self.target_dir_display} to upgrade.")

        cognite_module_version_raw = self._get_cognite_module_version(self.project_dir)
        self._cognite_module_version = parse_version(cognite_module_version_raw)
        changes = MigrationYAML.load()
        version_hash = next(
            (entry.cognite_modules_hash for entry in changes if entry.version == self._cognite_module_version),
            None,
        )
        if version_hash is None:
            raise ToolkitMigrationError(f"Failed to find migration from version {self._cognite_module_version!s}.")
        current_hash = calculate_directory_hash(self.project_dir / COGNITE_MODULES)
        self._has_changed_cognite_modules = current_hash != version_hash
        self._changes = changes.slice_from(self._cognite_module_version).as_one_change()

    @property
    def cognite_module_version(self) -> Version:
        return self._cognite_module_version

    def create_project_directory(self, clean: bool) -> None:
        if self.project_dir.exists():
            print(f"[bold]Upgrading directory {self.target_dir_display}...[/b]")
        else:
            raise ToolkitNotADirectoryError(f"Found no directory {self.target_dir_display} to upgrade.")

    def do_backup(self, no_backup: bool, verbose: bool) -> None:
        if not no_backup and self._has_changed_cognite_modules:
            print(
                "[bold yellow]WARNING:[/] The cognite_modules have changed, it will not be upgraded.\n"
                f"  No backup {'would have been' if self._dry_run else 'will be'} done."
            )
            return
        elif self._has_changed_cognite_modules:
            print("[bold yellow]WARNING:[/] The cognite_modules have changed, it will not be upgraded.")

        if not no_backup:
            prefix = "Would have backed up" if self._dry_run else "Backing up"
            if verbose:
                print(f"{prefix} {self.target_dir_display}")
            if not self._dry_run:
                backup_dir = tempfile.mkdtemp(prefix=f"{self.project_dir.name}.", suffix=".bck", dir=Path.cwd())
                shutil.copytree(self.project_dir, Path(backup_dir), dirs_exist_ok=True)
        else:
            print(
                "[bold yellow]WARNING:[/] --no-backup is specified, no backup "
                f"{'would have been' if self._dry_run else 'will be'} done."
            )

    def print_what_to_copy(self) -> None:
        if not self._has_changed_cognite_modules:
            print("  Will upgrade modules and files in place.")
            super().print_what_to_copy()

    def copy(self, verbose: bool) -> None:
        if self._has_changed_cognite_modules:
            return
        else:
            super().copy(verbose)

    def set_source(self, git_branch: str | None) -> None:
        if git_branch is None:
            return

        self._source = self._download_templates(git_branch, self._dry_run)

    def upsert_config_yamls(self, clean: bool) -> None:
        if clean:
            super().upsert_config_yamls(clean)
            return
        if self._has_changed_cognite_modules:
            return

        existing_environments = list(self.project_dir.glob("config.*.yaml"))
        if len(existing_environments) == 0:
            print("  [bold yellow]WARNING:[/] No existing config.[env].yaml files found, creating from the defaults.")
            super().upsert_config_yamls(clean)
            return

        config_yamls = ConfigYAMLs.load_existing_environments(existing_environments)

        config_yamls.load_default_variables(self._source)
        config_yamls.load_variables(self._source)

        for environment, config_yaml in config_yamls.items():
            config_filepath = self.project_dir / f"config.{environment}.yaml"
            for entry in config_yaml.added:
                print(entry)
            for entry in config_yaml.removed:
                print(entry)

            if self._dry_run:
                print(f"Would write {config_filepath.name!r} to {self.target_dir_display}")
            else:
                config_filepath.write_text(config_yaml.dump_yaml_with_comments(indent_size=2))
                print(f"Wrote {config_filepath.name!r} file to {self.target_dir_display}")

    def done_message(self) -> str:
        return (
            f"[bold green]Automatic upgrade of {self.target_dir_display} is done.[/]\n"
            "[bold red]You now have to do these manual steps:[/]"
        )

    def print_manual_steps(self) -> None:
        print(Panel("[bold]Manual Upgrade Steps[/]"))
        # If we updated the cognite_modules, we do not print the changes as there are no manual steps needed.
        self._changes.print(
            self.project_dir,
            str(self._cognite_module_version),
            print_cognite_module_changes=self._has_changed_cognite_modules,
        )

        print(
            Panel(
                f"[bold red]Make sure the version in _system.yaml is {current_version}[/]",
                title="When you are done with the manual updates",
            )
        )

    @staticmethod
    def _download_templates(git_branch: str, dry_run: bool) -> Path:
        toolkit_github_url = f"https://github.com/cognitedata/toolkit/archive/refs/heads/{git_branch}.zip"
        extract_dir = tempfile.mkdtemp(prefix="git.", suffix=".tmp", dir=Path.cwd())
        prefix = "Would download" if dry_run else "Downloading"
        print(f"{prefix} templates from https://github.com/cognitedata/toolkit, branch {git_branch}...")
        print(
            "  [bold yellow]WARNING:[/] You are only upgrading templates, not the cdf-tk tool. "
            "Your current version may not support the new templates."
        )
        if not dry_run:
            try:
                zip_path, _ = urllib.request.urlretrieve(toolkit_github_url)
                with zipfile.ZipFile(zip_path, "r") as f:
                    f.extractall(extract_dir)
            except Exception as e:
                raise ToolkitError(
                    f"Failed to download or extract templates. Are you sure that the branch {git_branch} exists in"
                    "the `https://github.com/cognitedata/toolkit` repository?"
                ) from e
        return Path(extract_dir) / f"cdf-project-templates-{git_branch}" / "cognite_toolkit"

    @classmethod
    def _get_cognite_module_version(cls, project_dir: Path) -> str:
        previous_version = None
        system_yaml_file = cls._search_system_yaml(project_dir)
        if system_yaml_file is not None:
            system_yaml = read_yaml_file(system_yaml_file)
            with suppress(KeyError):
                previous_version = system_yaml["cdf_toolkit_version"]

        elif (project_dir / "environments.yaml").exists():
            environments_yaml = read_yaml_file(project_dir / "environments.yaml")
            with suppress(KeyError):
                previous_version = environments_yaml["__system"]["cdf_toolkit_version"]

        if previous_version is None:
            raise ToolkitModuleVersionError(
                "Failed to load previous version, have you changed the "
                "'_system.yaml' or 'environments.yaml' (before 0.1.0b6) file?"
            )
        return previous_version

    @staticmethod
    def _search_system_yaml(project_dir: Path) -> Path | None:
        if (project_dir / "_system.yaml").exists():
            return project_dir / "_system.yaml"
        if (project_dir / COGNITE_MODULES / "_system.yaml").exists():
            # This is here to ensure that we check this path first
            return project_dir / COGNITE_MODULES / "_system.yaml"
        for path in project_dir.rglob("_system.yaml"):
            return path
        return None
