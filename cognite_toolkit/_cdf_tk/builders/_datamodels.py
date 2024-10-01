from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    ModuleLocation,
    SourceLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.loaders import GraphQLLoader
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning


class DataModelBuilder(Builder):
    _resource_folder = GraphQLLoader.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | list[ToolkitWarning]]:
        graphql_files = {
            source_file.source.path: source_file
            for source_file in source_files
            if source_file.source.path.suffix == ".graphql"
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

            extra_sources: list[SourceLocation] | None = None
            if loader is GraphQLLoader:
                extra_sources = self._add_graphql(loaded, source_file, graphql_files)

            destination_path = self._create_destination_path(source_file.source.path, module.dir, loader.kind)

            destination = BuildDestinationFile(
                path=destination_path,
                loaded=loaded,
                loader=loader,
                source=source_file.source,
                extra_sources=extra_sources,
            )
            yield destination

    def _add_graphql(
        self,
        loaded: dict[str, Any] | list[dict[str, Any]],
        source_file: BuildSourceFile,
        graphql_files: dict[Path, BuildSourceFile],
    ) -> list[SourceLocation]:
        extra_sources: list[SourceLocation] = []
        loaded_list = loaded if isinstance(loaded, list) else [loaded]
        for entry in loaded_list:
            if "dml" in entry:
                expected_name = entry["dml"]
            else:
                expected_name = (
                    f"{source_file.source.path.stem.removesuffix(GraphQLLoader.kind).removesuffix('.')}.graphql"
                )
            expected_path = source_file.source.path.parent / Path(expected_name)
            if expected_path in graphql_files:
                entry["dml"] = graphql_files[expected_path].content
                extra_sources.append(graphql_files[expected_path].source)
            else:
                raise ToolkitFileNotFoundError(
                    f"Failed to find GraphQL file. Expected {expected_name} adjacent to {source_file.source.path.as_posix()}"
                )
        return extra_sources
