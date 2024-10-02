import shutil
from collections.abc import Hashable, Sequence
from pathlib import Path

import yaml

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.data_classes import BuildVariables, BuiltResourceList, ModuleLocation
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError, ToolkitNotADirectoryError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import FunctionLoader
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_content, safe_read


class FunctionBuilder(Builder):
    _resource_folder = FunctionLoader.folder_name

    def build_resource_folder(
        self, resource_files: Sequence[Path], module_variables: BuildVariables, module: ModuleLocation
    ) -> BuiltResourceList[Hashable]:
        # In the 'functions' resource directories, all `.yaml` files must be in the root of the directory
        # This is to allow for function code to include arbitrary yaml files.
        # In addition, all files in not int the 'functions' directory are considered other files.
        resource_files = [file for file in resource_files if file.parent.name == self.resource_folder]

        built_resources = super().build_resource_folder(resource_files, module_variables, module)

        self._validate_function_directory(built_resources, module)
        self.copy_function_directory_to_build(built_resources, module.dir)

        return built_resources

    def _validate_function_directory(self, built_resources: BuiltResourceList, module: ModuleLocation) -> None:
        has_config_files = any(resource.kind == FunctionLoader.kind for resource in built_resources)
        if has_config_files:
            return
        config_files_misplaced = [
            file
            for file in module.source_paths_by_resource_folder[FunctionLoader.folder_name]
            if FunctionLoader.is_supported_file(file)
        ]
        if config_files_misplaced:
            for yaml_source_path in config_files_misplaced:
                required_location = module.dir / FunctionLoader.folder_name / yaml_source_path.name
                self.warn(
                    LowSeverityWarning(
                        f"The required Function resource configuration file "
                        f"was not found in {required_location.as_posix()!r}. "
                        f"The file {yaml_source_path.as_posix()!r} is currently "
                        f"considered part of the Function's artifacts and "
                        f"will not be processed by the Toolkit."
                    )
                )

    def copy_function_directory_to_build(
        self,
        built_resources: BuiltResourceList,
        module_dir: Path,
    ) -> None:
        function_directory_by_name = {
            dir_.name: dir_ for dir_ in (module_dir / FunctionLoader.folder_name).iterdir() if dir_.is_dir()
        }
        external_id_by_function_path = self._read_function_path_by_external_id(
            built_resources, function_directory_by_name
        )

        for external_id, function_path in external_id_by_function_path.items():
            # Function directory already validated to exist in read function
            function_dir = function_directory_by_name[external_id]
            destination = self.build_dir / FunctionLoader.folder_name / external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"Function {external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory."
                )
            shutil.copytree(function_dir, destination)

            # Clean up cache files
            for subdir in destination.iterdir():
                if subdir.is_dir():
                    shutil.rmtree(subdir / "__pycache__", ignore_errors=True)
            shutil.rmtree(destination / "__pycache__", ignore_errors=True)

    def _read_function_path_by_external_id(
        self, built_resources: BuiltResourceList, function_directory_by_name: dict[str, Path]
    ) -> dict[str, str | None]:
        function_path_by_external_id: dict[str, str | None] = {}
        for built_resource in built_resources:
            if built_resource.kind != FunctionLoader.kind or built_resource.destination is None:
                continue
            source_file = built_resource.destination
            try:
                raw_content = read_yaml_content(safe_read(source_file))
            except yaml.YAMLError as e:
                raise ToolkitYAMLFormatError(f"Failed to load function files {source_file.as_posix()!r} due to: {e}")
            raw_functions = raw_content if isinstance(raw_content, list) else [raw_content]
            for raw_function in raw_functions:
                external_id = raw_function.get("externalId")
                function_path = raw_function.get("functionPath")
                if not external_id:
                    self.warn(
                        HighSeverityWarning(
                            f"Function in {source_file.as_posix()!r} has no externalId defined. "
                            f"This is used to match the function to the function directory."
                        )
                    )
                    continue
                elif external_id not in function_directory_by_name:
                    raise ToolkitNotADirectoryError(
                        f"Function directory not found for externalId {external_id} defined in {source_file.as_posix()!r}."
                    )
                if not function_path:
                    self.warn(
                        MediumSeverityWarning(
                            f"Function {external_id} in {source_file.as_posix()!r} has no function_path defined."
                        )
                    )
                function_path_by_external_id[external_id] = function_path

        return function_path_by_external_id
