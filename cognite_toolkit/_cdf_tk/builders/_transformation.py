from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning


class TransformationBuilder(Builder):
    _resource_folder = TransformationLoader.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | list[ToolkitWarning]]:
        query_files = {
            source_file.source.path: source_file
            for source_file in source_files
            if source_file.source.path.suffix == ".sql"
        }

        for source_file in source_files:
            loaded = source_file.loaded
            if loaded is None:
                # Not a YAML file
                continue
            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

            if loader is TransformationLoader:
                self._add_query(loaded, source_file, query_files)

            destination_path = self._create_destination_path(source_file.source.path, module.dir, loader.kind)

            destination = BuildDestinationFile(
                path=destination_path,
                loaded=loaded,
                loader=loader,
                source=source_file.source,
                extra_sources=None,
            )
            yield destination

    def _add_query(
        self,
        loaded: dict[str, Any] | list[dict[str, Any]],
        source_file: BuildSourceFile,
        query_files: dict[Path, BuildSourceFile],
    ) -> None:
        loaded_list = loaded if isinstance(loaded, list) else [loaded]
        for entry in loaded_list:
            try:
                external_id = TransformationLoader.get_id(entry)
            except KeyError:
                # This will be validated later
                continue
            filepath = source_file.source.path
            query_file = self._get_query_file(filepath, external_id, query_files)

            if "query" in entry and query_file is not None:
                raise ToolkitYAMLFormatError(
                    f"query property is ambiguously defined in both the yaml file and a separate file named {query_file}\n"
                    f"Please remove one of the definitions, either the query property in {filepath} or the file {query_file}",
                )
            elif "query" not in entry and query_file is None:
                raise ToolkitYAMLFormatError(
                    f"query property or is missing. It can be inline or a separate file named {filepath.stem}.sql or {external_id}.sql",
                    filepath,
                )
            elif query_file is not None:
                entry["query"] = query_files

    @staticmethod
    def _get_query_file(
        source_file: Path, transformation_external_id: str | None, query_files: dict[Path, BuildSourceFile]
    ) -> str | None:
        query_file = source_file.parent / f"{source_file.stem}.sql"
        if query_file in query_files:
            return query_files[query_file].content
        if transformation_external_id:
            query_file = source_file.parent / f"{transformation_external_id}.sql"
            if query_file in query_files:
                return query_files[query_file].content
        return None