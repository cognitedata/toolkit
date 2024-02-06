from __future__ import annotations

import contextlib
import difflib
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from rich import print
from rich.panel import Panel

from cognite_toolkit.cdf_tk.load import ResourceLoader
from cognite_toolkit.cdf_tk.load._base_loaders import T_ID
from cognite_toolkit.cdf_tk.templates import (
    COGNITE_MODULES,
    build_config,
    create_local_config,
    iterate_modules,
    split_config,
)
from cognite_toolkit.cdf_tk.templates.data_classes import BuildConfigYAML, SystemYAML
from cognite_toolkit.cdf_tk.utils import CDFToolConfig, YAMLComment, YAMLWithComments


class ResourceYAML(YAMLWithComments[str, Any]):
    """This represents a YAML file that contains a single CDF resource such as transformation.

    It is used to load and dump an YAML file that contains comments.
    """

    def __init__(
        self, items: dict[str, Any] | None = None, comments: dict[tuple[str, ...], YAMLComment] | None = None
    ) -> None:
        super().__init__(items or {})
        self._comments = comments or {}

    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None:
        return self._comments.get(key)

    @classmethod
    def load(cls, content: str) -> ResourceYAML:
        comments = cls._extract_comments(content)
        items = yaml.safe_load(content)
        if not isinstance(items, dict):
            raise ValueError(f"Expected a dictionary, got {type(items)}")
        return cls(items, comments)

    def dump(self) -> dict[str, Any]:
        return self.data

    def dump_yaml_with_comments(self, indent_size: int = 2) -> str:
        """Dump a config dictionary to a yaml string"""
        return self._dump_yaml_with_comments(indent_size, False)


def pull_command(
    source_dir: str,
    id_: T_ID,
    env: str,
    dry_run: bool,
    verbose: bool,
    ToolGlobals: CDFToolConfig,
    Loader: type[ResourceLoader],
) -> None:
    if source_dir is None:
        source_dir = "./"
    source_path = Path(source_dir)
    if not source_path.is_dir():
        print(f"  [bold red]ERROR:[/] {source_path} does not exist")
        exit(1)

    build_dir = Path(tempfile.mkdtemp(prefix="build.", suffix=".tmp", dir=Path.cwd()))
    system_config = SystemYAML.load_from_directory(source_path / COGNITE_MODULES, env)
    config = BuildConfigYAML.load_from_directory(source_path, env)
    config.set_environment_variables()
    config.environment.selected_modules_and_packages = [module.name for module, _ in iterate_modules(source_path)]
    with contextlib.redirect_stdout(None):
        source_by_build_path = build_config(
            build_dir=build_dir,
            source_dir=source_path,
            config=config,
            system_config=system_config,
            clean=True,
            verbose=False,
        )

    loader = Loader.create_loader(ToolGlobals)
    resource_files = loader.find_files(build_dir / loader.folder_name)
    resource_by_file = {file: loader.load_resource(file, ToolGlobals, skip_validation=True) for file in resource_files}
    selected = {k: v for k, v in resource_by_file.items() if loader.get_id(v) == id_}
    if len(selected) == 0:
        print(f"  [bold red]ERROR:[/] No {loader.display_name} with external id {id_} governed in {source_dir}.")
        exit(1)
    elif len(selected) >= 2:
        files = "\n".join(map(str, selected.keys()))
        print(
            f"  [bold red]ERROR:[/] Multiple {loader.display_name} with {id_} found in {source_dir}. Delete all but one and try again."
            f"\nFiles: {files}"
        )
        exit(1)
    build_file, local_resource = next(iter(selected.items()))

    print(Panel(f"[bold]Pulling {loader.display_name} {id_}...[/]"))

    cdf_resources = loader.retrieve([loader.get_id(local_resource)])
    if not cdf_resources:
        print(f"  [bold red]ERROR:[/] No {loader.display_name} with {id_} found in CDF.")
    cdf_resource = cdf_resources[0].as_write()

    if cdf_resource == local_resource:
        print(f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} is up to date.")
        return

    source_file = source_by_build_path[build_file]

    cdf_dumped, extra_files = loader.dump_resource(cdf_resource, source_file)

    resource = ResourceYAML.load(build_file.read_text())
    resource.update(cdf_dumped)
    new_content = resource.dump_yaml_with_comments()

    module_dir = _get_module(source_file, loader.folder_name)

    local_config = create_local_config(split_config(config.modules), module_dir)
    new_content = reverse_replace_variables(new_content, local_config)

    if dry_run:
        print(
            f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} will be updated in "
            f"{source_file.relative_to(source_dir)!r}."
        )

    if dry_run or verbose:
        old_content = source_file.read_text()
        print("\n".join(difflib.unified_diff(old_content.splitlines(), new_content.splitlines())))

    if not dry_run:
        source_file.write_text(new_content)
        print(
            f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} updated in "
            f"{source_file.relative_to(source_dir)!r}."
        )

    for filepath, content in extra_files.items():
        content = reverse_replace_variables(content, local_config)
        if not filepath.exists():
            print(f"  [bold red]ERROR:[/] {filepath} does not exist.")
            continue

        if dry_run:
            print(f"  [bold green]INFO:[/] In addition, will update {filepath.relative_to(source_dir)!r}.")

        if dry_run or verbose:
            old_content = filepath.read_text()
            print("\n".join(difflib.unified_diff(old_content.splitlines(), new_content.splitlines())))

        if not dry_run:
            filepath.write_text(content)

    shutil.rmtree(build_dir)


def reverse_replace_variables(content: str, local_config: Mapping[str, str]) -> str:
    for name, variable in local_config.items():
        content = content.replace(str(variable), f"{{{{{name}}}}}")
    return content


def _get_module(source_file: Path, folder_name: str) -> Path:
    for i in range(len(source_file.parts)):
        if source_file.parts[i] == folder_name:
            module_dir = Path("/".join(source_file.parts[:i]))
            break
    else:
        raise ValueError(f"Could not find module directory for {source_file}. Contact support.")
    return module_dir
