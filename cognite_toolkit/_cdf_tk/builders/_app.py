import shutil
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.builders._base import Builder
from cognite_toolkit._cdf_tk.data_classes import (
    BuildDestinationFile,
    BuildSourceFile,
    BuiltResourceList,
    ModuleLocation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError, ToolkitNotADirectoryError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.app import AppIO
from cognite_toolkit._cdf_tk.tk_warnings import (
    FileReadWarning,
    HighSeverityWarning,
    LowSeverityWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.yaml_classes import AppsYAML


class AppBuilder(Builder):
    _resource_folder = AppIO.folder_name

    def __init__(self, build_dir: Path, warn: Callable[[ToolkitWarning], None]) -> None:
        super().__init__(build_dir, warn=warn)

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
                continue

            loader, warning = self._get_loader(source_file.source.path)
            if loader is None:
                if warning is not None:
                    yield [warning]
                continue

            warnings = WarningList[FileReadWarning]()
            if loader is AppIO:
                warnings = self.copy_app_directory_to_build(source_file)

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
        has_config_files = any(resource.kind == AppIO.kind for resource in built_resources)
        if has_config_files:
            return warnings
        config_files_misplaced = [
            file for file in module.source_paths_by_resource_folder[AppIO.folder_name] if AppIO.is_supported_file(file)
        ]
        if config_files_misplaced:
            for yaml_source_path in config_files_misplaced:
                required_location = module.dir / AppIO.folder_name / yaml_source_path.name
                warning = LowSeverityWarning(
                    f"The required App resource configuration file "
                    f"was not found in {required_location.as_posix()!r}. "
                    f"The file {yaml_source_path.as_posix()!r} is currently "
                    f"considered part of the App's artifacts and "
                    f"will not be processed by the Toolkit.",
                )
                warnings.append(warning)
        return warnings

    def copy_app_directory_to_build(self, source_file: BuildSourceFile) -> WarningList[FileReadWarning]:
        raw_content = source_file.loaded
        if raw_content is None:
            raise ToolkitValueError("App source file should be a YAML file.")
        raw_apps = raw_content if isinstance(raw_content, list) else [raw_content]
        warnings = WarningList[FileReadWarning]()
        for raw_app in raw_apps:
            try:
                app_config = AppsYAML.model_validate(raw_app)
            except ValidationError as exc:
                warnings.append(
                    HighSeverityWarning(
                        f"App in {source_file.source.path.as_posix()!r} has invalid configuration: {exc}",
                    )
                )
                continue

            app_external_id = app_config.app_external_id
            if app_config.source_path is not None:
                app_root = (source_file.source.path.parent / app_config.source_path).resolve()
            else:
                app_root = source_file.source.path.with_name(app_external_id)

            if not app_root.is_dir():
                raise ToolkitNotADirectoryError(
                    f"App directory not found for appExternalId {app_external_id!r} "
                    f"defined in {source_file.source.path.as_posix()!r}.",
                )

            # Prefer the pre-built dist/ subdirectory; fall back to the full app directory.
            dist_dir = app_root / "dist"
            source_dir = dist_dir if dist_dir.is_dir() else app_root

            destination = self.build_dir / self.resource_folder / app_external_id
            if destination.exists():
                raise ToolkitFileExistsError(
                    f"App {app_external_id!r} is duplicated. If this is unexpected, ensure you have a clean build directory.",
                )
            shutil.copytree(
                source_dir,
                destination,
                ignore=shutil.ignore_patterns("__pycache__", "node_modules", ".git"),
            )

        return warnings
