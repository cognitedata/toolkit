import shutil
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.cruds import FunctionCRUD
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    BuiltResourceList,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    RequirementsTXTValidationWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.utils import validate_requirements_with_pip


class FunctionBuilder(Builder):
    _resource_folder = FunctionCRUD.folder_name

    def __init__(self, build_dir: Path, warn: Callable[[ToolkitWarning], None]) -> None:
        super().__init__(build_dir, warn=warn)
        # Metrics for telemetry
        self.validation_count = 0
        self.validation_failures = 0
        self.validation_credential_errors = 0
        self.validation_time_ms = 0

    def _validate_function_requirements(
        self,
        requirements_txt: Path,
        raw_function: dict[str, Any],
        filepath: Path,
        external_id: str,
    ) -> RequirementsTXTValidationWarning | None:
        """Validate function requirements.txt using pip dry-run."""
        start_time = time.time()
        validation_result = validate_requirements_with_pip(
            requirements_txt_path=requirements_txt,
            index_url=raw_function.get("indexUrl"),
            extra_index_urls=raw_function.get("extraIndexUrls"),
        )
        elapsed_ms = int((time.time() - start_time) * 1000)
        self.validation_count += 1
        self.validation_time_ms += elapsed_ms

        if validation_result.success:
            return None

        self.validation_failures += 1
        if validation_result.is_credential_error:
            self.validation_credential_errors += 1

        return RequirementsTXTValidationWarning(
            filepath=filepath,
            external_id=external_id,
            error_details=validation_result.short_error,
            is_credential_error=validation_result.is_credential_error,
            resource="function",
        )

    def build(
        self,
        source_files: list[BuildSourceFile],
        module: ModuleLocation,
        console: Callable[[str], None] | None = None,
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
        self,
        built_resources: BuiltResourceList,
        module: ModuleLocation,
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
                    f"will not be processed by the Toolkit.",
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
                        f"This is used to match the function to the function directory.",
                    ),
                )
                continue
            if not function_path:
                warnings.append(
                    MediumSeverityWarning(
                        f"Function {external_id} in {source_file.source.path.as_posix()!r} has no function_path defined.",
                    ),
                )

            function_directory = source_file.source.path.with_name(external_id)

            if not function_directory.is_dir():
                raise ToolkitNotADirectoryError(
                    f"Function directory not found for externalId {external_id} defined in {source_file.source.path.as_posix()!r}.",
                )

            # Validate requirements.txt if present and feature is enabled
            if (
                Flags.FUNCTION_REQUIREMENTS_VALIDATION.is_enabled()
                and (requirements_txt := function_directory / "requirements.txt").exists()
            ):
                warning = self._validate_function_requirements(
                    requirements_txt,
                    raw_function,
                    source_file.source.path,
                    external_id,
                )
                if warning:
                    warnings.append(warning)

            destination = self.build_dir / self.resource_folder / external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"Function {external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory.",
                )
            shutil.copytree(function_directory, destination, ignore=shutil.ignore_patterns("__pycache__"))

        return warnings
