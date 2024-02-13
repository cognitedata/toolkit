from __future__ import annotations

import difflib
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from rich import print
from rich.panel import Panel

from cognite_toolkit.cdf_tk.load import ResourceLoader
from cognite_toolkit.cdf_tk.load._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit.cdf_tk.templates import (
    COGNITE_MODULES,
    build_config,
    iterate_modules,
)
from cognite_toolkit.cdf_tk.templates.data_classes import BuildConfigYAML, SystemYAML
from cognite_toolkit.cdf_tk.utils import CDFToolConfig, YAMLComment, YAMLWithComments


@dataclass
class ResourceProperty:
    """This represents a single property in a CDF resource file.

    Args:
        key_path: The path to the property in the resource file.
        build_value: The value of the property in the local resource file build file.
        cdf_value: The value of the property in the CDF resource.
        variable_placeholder: The placeholder for the variable, used to load the source file as a YAML.
        variable: The name of the variable used in the local resource file
        comment: The comment for the property.

    """

    key_path: tuple[str, ...]
    build_value: float | int | str | bool | None = None
    cdf_value: float | int | str | bool | None = None
    variable_placeholder: str | None = None
    variable: str | None = None
    comment: YAMLComment | None = None

    @property
    def is_different(self) -> bool:
        return self.build_value != self.cdf_value and self.build_value is not None and self.cdf_value is not None


class ResourceYAMLDifference(YAMLWithComments[tuple[str, ...], ResourceProperty]):
    """This represents a YAML file that contains resources and their properties.

    It is used to compare a local resource file with a CDF resource.
    """

    def __init__(self) -> None:
        super().__init__({})

    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None:
        return self[key].comment if key in self else None

    @classmethod
    def load(cls, build_content: str, source_content: str) -> ResourceYAMLDifference:
        raise NotImplementedError()
        # comments = cls._extract_comments(content)
        # items = yaml.safe_load(content)
        # if not isinstance(items, dict):
        #     raise ValueError(f"Expected a dictionary, got {type(items)}")
        # return cls()

    def dump(self) -> dict[str, Any]:
        raise NotImplementedError()
        # return self.data

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
    Loader: type[
        ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ]
    ],
) -> None:
    if source_dir is None:
        source_dir = "./"
    source_path = Path(source_dir)
    if not source_path.is_dir():
        print(f"  [bold red]ERROR:[/] {source_path} does not exist")
        exit(1)
    if not (source_path / COGNITE_MODULES).is_dir():
        print(
            f"  [bold red]ERROR:[/] {source_path} does not contain a {COGNITE_MODULES} directory. "
            f"Did you pass in a valid source directory?"
        )
        exit(1)

    build_dir = Path(tempfile.mkdtemp(prefix="build.", suffix=".tmp", dir=Path.cwd()))
    system_config = SystemYAML.load_from_directory(source_path / COGNITE_MODULES, env)
    config = BuildConfigYAML.load_from_directory(source_path, env)
    config.set_environment_variables()
    config.environment.selected_modules_and_packages = [module.name for module, _ in iterate_modules(source_path)]
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
    selected: dict[Path, T_WriteClass] = {}
    for file, resources in resource_by_file.items():
        if not isinstance(resources, Sequence):
            if loader.get_id(resources) == id_:  # type: ignore[arg-type]
                selected[file] = resources  # type: ignore[assignment]
            continue
        for resource in resources:
            if loader.get_id(resource) == id_:
                selected[file] = resource
                break
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
        exit(1)
    cdf_resource = cdf_resources[0].as_write()

    if cdf_resource == local_resource:
        print(f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} is up to date.")
        return

    source_file = source_by_build_path[build_file]

    cdf_dumped, extra_files = loader.dump_resource(cdf_resource, source_file, local_resource)

    # Using the ResourceYAML class to load and dump the file to preserve comments
    resource = ResourceYAMLDifference.load(build_file.read_text(), source_file.read_text())
    resource.update(cdf_dumped)
    # if Loader is NodeLoader:
    #     # Nodes have a special format that needs to be preserved
    #     for no, node in enumerate(resource["nodes"]):
    #         if NodeId(node.get("space"), node.get("externalId")) == id_:
    #             resource["nodes"][no].update(node)
    #             break
    #     else:
    #         raise ValueError(f"Node with id {id_} not found in {source_file}.")
    # else:
    #     resource.update(cdf_dumped)

    new_content = resource.dump_yaml_with_comments()

    if dry_run:
        print(
            f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_!r} will be updated in "
            f"'{source_file.relative_to(source_dir)}'."
        )

    if verbose:
        old_content = source_file.read_text()
        print(
            Panel(
                "\n".join(difflib.unified_diff(old_content.splitlines(), new_content.splitlines())),
                title=f"Difference between local and CDF resource {source_file.name!r}",
            )
        )

    if not dry_run:
        source_file.write_text(new_content)
        print(
            f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} updated in "
            f"'{source_file.relative_to(source_dir)}'."
        )

    for filepath, content in extra_files.items():
        if not filepath.exists():
            print(f"  [bold red]ERROR:[/] {filepath} does not exist.")
            continue

        if dry_run:
            print(f"[bold green]INFO:[/] In addition, would update '{filepath.relative_to(source_dir)}'.")

        if verbose:
            old_content = filepath.read_text()
            print(
                Panel(
                    "\n".join(difflib.unified_diff(old_content.splitlines(), content.splitlines())),
                    title=f"Difference between local and CDF resource {filepath.name!r}",
                )
            )

        if not dry_run:
            filepath.write_text(content)

    shutil.rmtree(build_dir)
    print("  [bold green]INFO:[/] Pull complete. Cleaned up temporary files.")
