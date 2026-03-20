from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.cruds import RuleSetVersionCRUD
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    ModuleLocation,
    SourceLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning
from cognite_toolkit._cdf_tk.utils import safe_write


class RuleSetBuilder(Builder):
    _resource_folder = RuleSetVersionCRUD.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | list[ToolkitWarning]]:
        ttl_files = {
            source_file.source.path: source_file
            for source_file in source_files
            if source_file.source.path.suffix == ".ttl"
        }

        for source_file in source_files:
            loaded = source_file.loaded
            if loaded is None:
                continue
            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

            destination_path = self._create_destination_path(source_file.source.path, loader.kind)

            extra_sources: list[SourceLocation] | None = None
            if loader is RuleSetVersionCRUD:
                extra_sources = self._add_rules(loaded, source_file, ttl_files, destination_path)

            destination = BuildDestinationFile(
                path=destination_path,
                loaded=loaded,
                loader=loader,
                source=source_file.source,
                extra_sources=extra_sources,
            )
            yield destination

    def load_extra_field(self, extra: str) -> tuple[str, Any]:
        return "rules", [extra]

    def _add_rules(
        self,
        loaded: dict[str, Any] | list[dict[str, Any]],
        source_file: BuildSourceFile,
        ttl_files: dict[Path, BuildSourceFile],
        ruleset_destination_path: Path,
    ) -> list[SourceLocation]:
        loaded_list = loaded if isinstance(loaded, list) else [loaded]
        extra_sources: list[SourceLocation] = []
        for entry in loaded_list:
            try:
                id_ = RuleSetVersionCRUD.get_id(entry)
            except KeyError:
                continue
            filepath = source_file.source.path
            ttl_file = self._get_ttl_file(filepath, id_.rule_set_external_id, ttl_files)

            if "rules" in entry and ttl_file is not None:
                raise ToolkitYAMLFormatError(
                    f"'rules' is defined in both the YAML and a separate file named {ttl_file.source.path}\n"
                    f"Please remove one: either the inline 'rules' in {filepath} or the file {ttl_file.source.path}",
                )
            if "rules" not in entry and ttl_file is None:
                raise ToolkitFileNotFoundError(
                    f"'rules' is missing and no .ttl file found. Expected {filepath.stem}.ttl or {id_.rule_set_external_id}.ttl next to {filepath}",
                    filepath,
                )
            if ttl_file is not None:
                destination_path = self._create_destination_path(ttl_file.source.path, "Rules")
                safe_write(destination_path, ttl_file.content, encoding=BUILD_FOLDER_ENCODING)
                entry["rules"] = [ttl_file.content]
                extra_sources.append(ttl_file.source)

        return extra_sources

    @staticmethod
    def _get_ttl_file(
        source_file: Path, rule_set_external_id: str | None, ttl_files: dict[Path, BuildSourceFile]
    ) -> BuildSourceFile | None:
        ttl_path = source_file.parent / f"{source_file.stem}.ttl"
        if ttl_path in ttl_files:
            return ttl_files[ttl_path]
        if rule_set_external_id:
            ttl_path = source_file.parent / f"{rule_set_external_id}.ttl"
            if ttl_path in ttl_files:
                return ttl_files[ttl_path]
        return None
