from collections import defaultdict
from collections.abc import Iterable, Sequence, Set
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND,
    Loader,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitValidationError, ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning
from cognite_toolkit._cdf_tk.tk_warnings.other import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


@dataclass
class DeployOptions:
    dry_run: bool = False
    include: Sequence[str] | None = None
    force_update: bool = False
    verbose: bool = False


@dataclass
class ResourceDirectory:
    directory: Path
    files_by_crud: dict[type[ResourceCRUD], list[Path]] = field(default_factory=lambda: defaultdict(list))
    invalid_files: list[Path] = field(default_factory=list)


@dataclass
class ReadBuildDirectory:
    build_dir: Path
    resource_directories: list[ResourceDirectory]
    skipped_directories: list[ResourceDirectory]
    invalid_directories: list[Path]
    is_strict_validation: bool = False

    def create_warnings(self) -> Iterable[ToolkitWarning]:
        raise NotImplementedError()

    def skipped_cruds(self) -> set[type[ResourceCRUD]]:
        return {crud for dir in self.skipped_directories for crud in dir.files_by_crud.keys()}

    def as_files_by_crud(self) -> dict[type[ResourceCRUD], list[Path]]:
        files_by_crud: dict[type[ResourceCRUD], list[Path]] = {}
        for dir in self.resource_directories:
            files_by_crud.update(dir.files_by_crud)
        return files_by_crud


@dataclass
class DeploymentStep:
    loader_cls: type[Loader]
    files: list[Path]


@dataclass
class DeploymentResult: ...


class DeployV2Command(ToolkitCommand):
    def deploy(
        self,
        env_vars: EnvironmentVariables,
        build_dir: Path,
        options: DeployOptions | None = None,
    ) -> Any:
        read_dir = self._read_build_directory(build_dir, options)

        client = env_vars.get_client(is_strict_validation=read_dir.is_strict_validation)

        for warning in read_dir.create_warnings():
            self.warn(warning, console=client.console)

        options = options or DeployOptions()

        plan = self._create_deployment_plan(read_dir)

        self._display_plan(plan)

        results = self._apply_plan(env_vars, plan, options.dry_run, options.force_update)

        self._display_results(results)

        return results

    @classmethod
    def _read_build_directory(cls, build_dir: Path, options: DeployOptions | None = None) -> ReadBuildDirectory:
        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"Build directory {build_dir!s} does not exist.")
        available_resource_types = set(RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND.keys())
        if options and options.include:
            if invalid := set(options.include) - available_resource_types:
                raise ToolkitValidationError(
                    f"Invalid resource types specified: {humanize_collection(invalid)}, available types: {humanize_collection(available_resource_types)}"
                )
        # Todo: Check linage file.
        include = set(options.include) if options and options.include else None
        invalid_resource_dirs: list[Path] = []
        resource_directories: list[ResourceDirectory] = []
        skipped_resource_dirs: list[ResourceDirectory] = []
        for resource_dir in build_dir.iterdir():
            if not resource_dir.is_dir():
                continue
            if resource_dir.name not in RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND:
                invalid_resource_dirs.append(resource_dir)
                continue
            resources = ResourceDirectory(resource_dir)
            crud_by_kind = RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[resource_dir.name]
            for yaml_file in resource_dir.glob("*.yaml"):
                for kind, crud in crud_by_kind.items():
                    if yaml_file.stem.endswith(kind):
                        resources.files_by_crud[crud].append(yaml_file)
                        break
                else:
                    resources.invalid_files.append(yaml_file)
            if include is not None and resource_dir.name not in include:
                skipped_resource_dirs.append(resources)
            else:
                resource_directories.append(resources)

        if not resource_directories:
            raise ToolkitValueError(f"No resources found in {build_dir.as_posix()} directory.")

        return ReadBuildDirectory(
            build_dir=build_dir,
            resource_directories=resource_directories,
            invalid_directories=invalid_resource_dirs,
            skipped_directories=skipped_resource_dirs,
        )

    def _create_deployment_plan(self, read_dir: ReadBuildDirectory) -> list[DeploymentStep]:
        files_by_crud = read_dir.as_files_by_crud()
        skipped_cruds = read_dir.skipped_cruds()
        dependencies_by_crud: dict[type[ResourceCRUD], Set[type[ResourceCRUD]]] = {}
        should_not_have_skipped: set[type[ResourceCRUD]] = set()
        for crud_cls in files_by_crud.keys():
            dependencies = crud_cls.dependencies
            if missing := (skipped_cruds.intersection(dependencies)):
                should_not_have_skipped.update(missing)
            dependencies_by_crud[crud_cls] = dependencies

        if should_not_have_skipped:
            self.warn(
                HighSeverityWarning(
                    f"You have skipped {humanize_collection({crud_cls.folder_name for crud_cls in should_not_have_skipped})}, which are required dependencies for other included resource types. This may cause the deployment to fail."
                    f"Run without specifying `--include` to not skip any resource types."
                )
            )

        try:
            ordered = list(TopologicalSorter(dependencies_by_crud).static_order())
        except CycleError as e:
            raise RuntimeError("Bug in Toolkit. Cyclic dependencies in support resource types detected.") from e

        plan: list[DeploymentStep] = []
        for step in ordered:
            if step not in files_by_crud:
                continue
            plan.append(
                DeploymentStep(
                    loader_cls=step,
                    files=files_by_crud[step],
                )
            )
        return plan

    def _display_plan(self, plan: list[DeploymentStep]) -> None:
        raise NotImplementedError()

    def _apply_plan(
        self, env_vars: EnvironmentVariables, plan: list[DeploymentStep], dry_run: bool, force_update: bool
    ) -> Sequence[DeploymentResult]:
        raise NotImplementedError()

    def _display_results(self, results: Sequence[DeploymentResult]) -> None:
        raise NotImplementedError()
