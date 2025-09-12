import shutil
from collections.abc import Callable, Iterable, Sequence

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.cruds import FunctionCRUD
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    BuiltResourceList,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    ToolkitWarning,
    WarningList,
)


class FunctionBuilder(Builder):
    _resource_folder = FunctionCRUD.folder_name

    def build(
        self, source_files: list[BuildSourceFile], module: ModuleLocation, console: Callable[[str], None] | None = None
    ) -> Iterable[BuildDestinationFile | Sequence[ToolkitWarning]]:
        for source_file in source_files:
            if source_file.loaded is None:
                continue
            if source_file.source.path.parent.parent != module.dir:
                # Function YAML files must be in the resource folder.
                continue

            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

            warnings = WarningList[FileReadWarning]()
            if loader is FunctionCRUD:
                warnings = self.copy_function_directory_to_build(source_file)

            destination_path = self._create_destination_path(source_file.source.path, loader.kind)

            yield BuildDestinationFile(
                path=destination_path,
                loaded=source_file.loaded,
                loader=loader,
                source=source_file.source,
                extra_sources=None,
                warnings=warnings,
            )

    def validate_directory(
        self, built_resources: BuiltResourceList, module: ModuleLocation
    ) -> WarningList[ToolkitWarning]:
        warnings = WarningList[ToolkitWarning]()
        has_config_files = any(resource.kind == FunctionCRUD.kind for resource in built_resources)
        if has_config_files:
            return warnings
        config_files_misplaced = [
            file
            for file in module.source_paths_by_resource_folder[FunctionCRUD.folder_name]
            if FunctionCRUD.is_supported_file(file)
        ]
        if config_files_misplaced:  # and not has_config_files:
            for yaml_source_path in config_files_misplaced:
                required_location = module.dir / FunctionCRUD.folder_name / yaml_source_path.name
                warning = LowSeverityWarning(
                    f"The required Function resource configuration file "
                    f"was not found in {required_location.as_posix()!r}. "
                    f"The file {yaml_source_path.as_posix()!r} is currently "
                    f"considered part of the Function's artifacts and "
                    f"will not be processed by the Toolkit."
                )
                warnings.append(warning)
        return warnings

    def copy_function_directory_to_build(self, source_file: BuildSourceFile) -> WarningList[FileReadWarning]:
        raw_content = source_file.loaded
        if raw_content is None:
            # This should already be checked before calling this method.
            raise ToolkitValueError("Function source file should be a YAML file.")
        raw_functions = raw_content if isinstance(raw_content, list) else [raw_content]
        warnings = WarningList[FileReadWarning]()
        for raw_function in raw_functions:
            external_id = raw_function.get("externalId")
            function_path = raw_function.get("functionPath")
            if not external_id:
                warnings.append(
                    HighSeverityWarning(
                        f"Function in {source_file.source.path.as_posix()!r} has no externalId defined. "
                        f"This is used to match the function to the function directory."
                    )
                )
                continue
            if not function_path:
                warnings.append(
                    MediumSeverityWarning(
                        f"Function {external_id} in {source_file.source.path.as_posix()!r} has no function_path defined."
                    )
                )

            function_directory = source_file.source.path.with_name(external_id)

            if not function_directory.is_dir():
                raise ToolkitNotADirectoryError(
                    f"Function directory not found for externalId {external_id} defined in {source_file.source.path.as_posix()!r}."
                )

            destination = self.build_dir / self.resource_folder / external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"Function {external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory."
                )
            shutil.copytree(function_directory, destination, ignore=shutil.ignore_patterns("__pycache__"))

        return warnings
