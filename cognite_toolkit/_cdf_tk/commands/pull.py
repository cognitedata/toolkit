from __future__ import annotations

import difflib
import re
import uuid
from collections import UserList
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union

import yaml
from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from rich import print
from rich.markdown import Markdown
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, SystemYAML
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitDuplicatedResourceError,
    ToolkitMissingResourceError,
    ToolkitNotADirectoryError,
)
from cognite_toolkit._cdf_tk.loaders import ResourceLoader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, YAMLComment, YAMLWithComments, tmp_build_directory

from ._base import ToolkitCommand

_VARIABLE_PATTERN = re.compile(r"\{\{(.+?)\}\}")
# The encoding and newline characters to use when writing files
# These are hardcoded to ensure that running the pull command on different platforms
# will produce the same output. The motivation is when having local sources in
# version control, the diff will be easier to read.
ENCODING = "utf-8"
NEWLINE = "\n"


@dataclass
class Variable:
    placeholder: str | None = None
    name: str | None = None
    source_value: str | None = None


@dataclass
class ResourceProperty:
    """This represents a single property in a CDF resource file.

    Args:
        key_path: The path to the property in the resource file.
        build_value: The value of the property in the local resource file build file.
        cdf_value: The value of the property in the CDF resource.
        variables: A list of variables that are used in the property value.
    """

    key_path: tuple[str | int, ...]
    build_value: float | int | str | bool | None = None
    cdf_value: float | int | str | bool | None = None
    variables: list[Variable] = field(default_factory=list)

    @property
    def value(self) -> float | int | str | bool | None:
        if self.has_variables:
            return self.variables[0].source_value
        return self.cdf_value or self.build_value

    @property
    def has_variables(self) -> bool:
        return bool(self.variables)

    @property
    def is_changed(self) -> bool:
        return (
            self.build_value != self.cdf_value
            and self.build_value is not None
            and self.cdf_value is not None
            and not self.has_variables
        )

    @property
    def is_added(self) -> bool:
        return self.build_value is None and self.cdf_value is not None

    @property
    def is_cannot_change(self) -> bool:
        return (
            self.build_value != self.cdf_value
            and not self.has_variables
            and self.build_value is not None
            and self.cdf_value is not None
        )

    def __str__(self) -> str:
        key_str = ".".join(map(str, self.key_path))
        if self.is_added:
            return f"ADDED: '{key_str}: {self.cdf_value}'"
        elif self.is_changed:
            return f"CHANGED: '{key_str}: {self.build_value} -> {self.cdf_value}'"
        elif self.is_cannot_change:
            return f"CANNOT CHANGE (contains variables): '{key_str}: {self.build_value} -> {self.cdf_value}'"
        else:
            return f"UNCHANGED: '{key_str}: {self.build_value}'"


class ResourceYAMLDifference(YAMLWithComments[tuple[Union[str, int], ...], ResourceProperty]):
    """This represents a YAML file that contains resources and their properties.

    It is used to compare a local resource file with a CDF resource.
    """

    def __init__(
        self,
        items: dict[tuple[str | int, ...], ResourceProperty],
        comments: dict[tuple[str, ...], YAMLComment] | None = None,
    ) -> None:
        super().__init__(items or {})
        self._comments = comments or {}

    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None:
        return self._comments.get(key)

    @classmethod
    def load(cls, build_content: str, source_content: str) -> ResourceYAMLDifference:
        comments = cls._extract_comments(build_content)
        build = yaml.safe_load(build_content)
        build_flatten = cls._flatten(build)
        items: dict[tuple[str | int, ...], ResourceProperty] = {}
        for key, value in build_flatten.items():
            items[key] = ResourceProperty(
                key_path=key,
                build_value=value,
            )

        source_content, variable_by_placeholder = cls._replace_variables(source_content)
        source = yaml.safe_load(source_content)
        source_items = cls._flatten(source)
        for key, value in source_items.items():
            for placeholder, variable in variable_by_placeholder.items():
                if placeholder in str(value):
                    items[key].variables.append(
                        Variable(
                            placeholder=placeholder,
                            name=variable_by_placeholder[placeholder],
                            source_value=str(value),
                        )
                    )
        return cls(items, comments)

    @classmethod
    def _flatten(
        cls, raw: dict[str, Any] | list[dict[str, Any]]
    ) -> dict[tuple[str | int, ...], str | int | float | bool | None]:
        if isinstance(raw, dict):
            return cls._flatten_dict(raw)
        elif isinstance(raw, list):
            raise NotImplementedError()
        else:
            raise ValueError(f"Expected a dictionary or list, got {type(raw)}")

    @classmethod
    def _flatten_dict(
        cls, raw: dict[str, Any], key_path: tuple[str | int, ...] = ()
    ) -> dict[tuple[str | int, ...], str | int | float | bool | None]:
        items: dict[tuple[str | int, ...], str | int | float | bool | None] = {}
        for key, value in raw.items():
            if key == "scopes":
                # Hack to handle that scopes is a list variable
                items[(*key_path, key)] = value
            elif isinstance(value, dict):
                items.update(cls._flatten_dict(value, (*key_path, key)))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.update(cls._flatten_dict(item, (*key_path, key, i)))
                    else:
                        items[(*key_path, key, i)] = item
            else:
                items[(*key_path, key)] = value
        return items

    @classmethod
    def _replace_variables(cls, content: str) -> tuple[str, dict[str, str]]:
        variable_by_placeholder: dict[str, str] = {}
        seen: set[str] = set()
        for match in _VARIABLE_PATTERN.finditer(content):
            variable = match.group(1)
            if variable in seen:
                continue
            placeholder = f"VARIABLE_{uuid.uuid4().hex[:8]}"
            content = content.replace(f"{{{{{variable}}}}}", placeholder)
            variable_by_placeholder[placeholder] = variable
            seen.add(variable)
        return content, variable_by_placeholder

    def update_cdf_resource(self, cdf_resource: dict[str, Any]) -> None:
        for key, value in self._flatten_dict(cdf_resource).items():
            if key in self:
                self[key].cdf_value = value
            else:
                self[key] = ResourceProperty(key_path=key, cdf_value=value)

    def dump(self) -> dict[Any, Any]:
        dumped: dict[Any, Any] = {}
        for key, prop in self.items():
            current = dumped
            for part, next_part in zip(key[:-1], key[1:]):
                if isinstance(part, int) and isinstance(current, list) and len(current) < part + 1:
                    current.append({})
                    current = current[part]
                elif isinstance(part, int) and isinstance(current, list) and part < len(current):
                    current = current[part]
                elif isinstance(part, str) and isinstance(next_part, str):
                    current = current.setdefault(part, {})
                elif isinstance(part, str) and isinstance(next_part, int):
                    current = current.setdefault(part, [])
                else:
                    raise ValueError(f"Expected a string or int, got {type(part)}")
            if isinstance(key[-1], int) and isinstance(current, list):
                current.append(prop.value)
            elif isinstance(key[-1], str) and isinstance(current, dict):
                current[key[-1]] = prop.value
            else:
                raise ValueError(f"Expected a string or int, got {type(key[-1])}")
        return dumped

    def dump_yaml_with_comments(self, indent_size: int = 2) -> str:
        """Dump a config dictionary to a yaml string"""
        dumped_with_comments = self._dump_yaml_with_comments(indent_size, False)
        for key, prop in self.items():
            for variable in prop.variables:
                if variable.placeholder:
                    dumped_with_comments = dumped_with_comments.replace(
                        variable.placeholder, f"{{{{{variable.name}}}}}"
                    )
        return dumped_with_comments

    def display(self, title: str | None = None) -> None:
        added = [prop for prop in self.values() if prop.is_added]
        changed = [prop for prop in self.values() if prop.is_changed]
        cannot_change = [prop for prop in self.values() if prop.is_cannot_change]
        unchanged = [
            prop for prop in self.values() if not prop.is_added and not prop.is_changed and not prop.is_cannot_change
        ]

        content: list[str] = []
        if added:
            content.append("\n**Added properties**(Either set in CDF UI or default values set by CDF):")
            content.extend([f" - {prop}" for prop in added])
        if changed:
            content.append("\n**Changed properties:**")
            content.extend([f" - {prop}" for prop in changed])
        if cannot_change:
            content.append("\n**Cannot change properties**")
            content.extend([f" - {prop}" for prop in cannot_change])
        if unchanged:
            content.append(f"\n**{len(unchanged)} properties unchanged**")

        print(Panel.fit(Markdown("\n".join(content), justify="left"), title=title or "Resource differences"))


@dataclass
class Line:
    line_no: int
    build_value: str | None = None
    source_value: str | None = None
    cdf_value: str | None = None
    variables: list[str] | None = None

    @property
    def value(self) -> str:
        if self.variables:
            if self.source_value is None:
                raise ValueError("Source value should be set if there are variables")
            return self.source_value
        value = self.cdf_value or self.build_value
        if value is None:
            raise ValueError("CDF value or build value should be set")
        return value

    @property
    def is_changed(self) -> bool:
        return (
            self.build_value != self.cdf_value
            and self.build_value is not None
            and self.cdf_value is not None
            and self.variables is None
        )

    @property
    def is_added(self) -> bool:
        return self.build_value is None and self.cdf_value is not None

    @property
    def is_cannot_change(self) -> bool:
        return (
            self.build_value != self.cdf_value
            and self.variables is not None
            and self.build_value is not None
            and self.cdf_value is not None
        )


class TextFileDifference(UserList):
    def __init__(self, lines: list[Line] | None) -> None:
        super().__init__(lines or [])

    @classmethod
    def load(cls, build_content: str, source_content: str) -> TextFileDifference:
        lines = []
        # Build and source content should have the same number of lines
        for no, (build, source) in enumerate(zip(build_content.splitlines(), source_content.splitlines())):
            variables = [v.group(1) for v in _VARIABLE_PATTERN.finditer(source)] or None
            lines.append(
                Line(
                    line_no=no + 1,
                    build_value=build,
                    source_value=source,
                    variables=variables,
                )
            )
        return cls(lines)

    def update_cdf_content(self, cdf_content: str) -> None:
        for i, line in enumerate(cdf_content.splitlines()):
            if i < len(self):
                self[i].cdf_value = line
            else:
                self.append(Line(cdf_value=line, line_no=i + 1))

    def dump(self) -> str:
        return "\n".join(line.value for line in self) + "\n"

    def display(self, title: str | None = None) -> None:
        added = [line for line in self if line.is_added]
        changed = [line for line in self if line.is_changed]
        cannot_change = [line for line in self if line.is_cannot_change]
        unchanged_count = len(self) - len(added) - len(changed) - len(cannot_change)

        content: list[str] = []
        if added:
            content.append("\n**Added lines**")
            if len(added) == 1:
                content.append(f" - Line {added[0].line_no}: '{added[0].cdf_value}'")
            else:
                content.append(f" - Line {added[0].line_no} - {added[-1].line_no}: {len(added)} lines")
        if changed:
            content.append("\n**Changed lines**")
            if len(changed) == 1:
                content.append(f" - Line {changed[0].line_no}: '{changed[0].source_value}' -> '{changed[0].cdf_value}'")
            else:
                content.append(f" - Line {changed[0].line_no} - {changed[-1].line_no}: {len(changed)} lines")
        if cannot_change:
            content.append("\n**Cannot change lines**")
            if len(cannot_change) == 1:
                content.append(
                    f" - Line {cannot_change[0].line_no}: '{cannot_change[0].source_value}' -> '{cannot_change[0].cdf_value}'"
                )
            else:
                content.append(
                    f" - Line {cannot_change[0].line_no} - {cannot_change[-1].line_no}: {len(cannot_change)} lines"
                )
        if unchanged_count != 0:
            content.append(f"\n**{unchanged_count} lines unchanged**")

        print(Panel.fit(Markdown("\n".join(content), justify="left"), title=title or "File differences"))


class PullCommand(ToolkitCommand):
    def execute(
        self,
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
            source_dir = "../"
        source_path = Path(source_dir)
        if not source_path.is_dir():
            raise ToolkitNotADirectoryError(str(source_path))

        with tmp_build_directory() as build_dir:
            system_config = SystemYAML.load_from_directory(source_path, env)
            config = BuildConfigYAML.load_from_directory(source_path, env)
            config.set_environment_variables()
            # Todo Remove once the new modules in `_cdf_tk/prototypes/_packages` are finished.
            config.variables.pop("_cdf_tk", None)
            # Todo do not use the variables to find the available modules.
            config.environment.selected = config.available_modules
            print(
                Panel.fit(
                    f"[bold]Building all modules found in {config.filepath} (not only the modules under "
                    f"'selected_modules_and_packages') from {source_path}...[/]"
                )
            )
            source_by_build_path = BuildCommand().build_config(
                build_dir=build_dir,
                source_dir=source_path,
                config=config,
                system_config=system_config,
                clean=True,
                verbose=False,
            )

            loader = Loader.create_loader(ToolGlobals, build_dir)
            resource_files = loader.find_files()
            resource_by_file = {
                file: loader.load_resource(file, ToolGlobals, skip_validation=True) for file in resource_files
            }
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
                raise ToolkitMissingResourceError(
                    f"No {loader.display_name} with external id {id_} found in the current configuration in {source_dir}."
                )
            elif len(selected) >= 2:
                files = "\n".join(map(str, selected.keys()))
                raise ToolkitDuplicatedResourceError(
                    f"Multiple {loader.display_name} with {id_} found in {source_dir}. Delete all but one and try again. "
                    f"Files: {files}"
                )
            build_file, local_resource = next(iter(selected.items()))

            print(f"[bold]Pulling {loader.display_name} {id_}...[/]")

            resource_id = loader.get_id(local_resource)
            cdf_resources = loader.retrieve([resource_id])
            if not cdf_resources:
                raise ToolkitMissingResourceError(f"No {loader.display_name} with {id_} found in CDF.")

            cdf_resource = cdf_resources[0].as_write()
            if cdf_resource == local_resource:
                print(f"  [bold green]INFO:[/] {loader.display_name.capitalize()} {id_} is up to date.")
                return

            source_file = source_by_build_path[build_file]

            cdf_dumped, extra_files = loader.dump_resource(cdf_resource, source_file, local_resource)

            # Using the ResourceYAML class to load and dump the file to preserve comments and detect changes
            resource = ResourceYAMLDifference.load(build_file.read_text(), source_file.read_text())
            resource.update_cdf_resource(cdf_dumped)

            resource.display(title=f"Resource differences for {loader.display_name} {id_}")
            new_content = resource.dump_yaml_with_comments()

            if dry_run:
                print(
                    f"[bold green]INFO:[/] {loader.display_name.capitalize()} {id_!r} will be updated in file "
                    f"'{source_file.relative_to(source_dir)}'."
                )

            if verbose:
                old_content = source_file.read_text()
                print(
                    Panel(
                        "\n".join(difflib.unified_diff(old_content.splitlines(), new_content.splitlines())),
                        title=f"Updates to file {source_file.name!r}",
                    )
                )

            if not dry_run:
                with source_file.open(mode="w", encoding=ENCODING, newline=NEWLINE) as f:
                    f.write(new_content)
                print(
                    f"[bold green]INFO:[/] {loader.display_name.capitalize()} {id_} updated in "
                    f"'{source_file.relative_to(source_dir)}'."
                )

            for filepath, content in extra_files.items():
                if not filepath.exists():
                    print(f"[bold red]ERROR:[/] {filepath} does not exist.")
                    continue

                build_extra_file = Path(build_dir / loader.folder_name / filepath.name)
                if not build_extra_file.exists():
                    print(f"[bold red]ERROR:[/] {build_extra_file} does not exist.")
                    continue

                file_diffs = TextFileDifference.load(build_extra_file.read_text(), filepath.read_text())
                file_diffs.update_cdf_content(content)

                has_changed = any(line.is_added or line.is_changed for line in file_diffs)
                if dry_run:
                    if has_changed:
                        print(
                            f"[bold green]INFO:[/] In addition, would update file '{filepath.relative_to(source_dir)}'."
                        )
                    else:
                        print(
                            f"[bold green]INFO:[/] File '{filepath.relative_to(source_dir)}' has not changed, "
                            "thus no update would have been done."
                        )

                if verbose:
                    old_content = filepath.read_text()
                    print(
                        Panel(
                            "\n".join(difflib.unified_diff(old_content.splitlines(), content.splitlines())),
                            title=f"Difference between local and CDF resource {filepath.name!r}",
                        )
                    )

                if not dry_run and has_changed:
                    with filepath.open(mode="w", encoding=ENCODING, newline=NEWLINE) as f:
                        f.write(content)
                    print(f"[bold green]INFO:[/] File '{filepath.relative_to(source_dir)}' updated.")

        print("[bold green]INFO:[/] Pull complete. Cleaned up temporary files.")
