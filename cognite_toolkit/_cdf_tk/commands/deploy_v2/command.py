from collections import defaultdict
from collections.abc import Iterable, Sequence, Set
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Generic

from rich.progress import Progress

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND,
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
    crud_cls: type[ResourceCRUD]
    files: list[Path]


@dataclass
class ResourceToDeploy(Generic[T_Identifier, T_RequestResource]):
    to_create: list[T_RequestResource]
    to_delete: list[T_Identifier]
    to_update: list[T_RequestResource]
    unchanged: list[T_Identifier]


@dataclass
class DeploymentResult:
    is_dry_run: bool
    created: int
    deleted: int
    updated: int
    unchanged: int


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

        self._display_plan(client, plan)

        results = self._apply_plan(client, env_vars, plan, options.dry_run, options.force_update)

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
            skipped_str = humanize_collection({crud_cls.folder_name for crud_cls in should_not_have_skipped})
            self.warn(
                HighSeverityWarning(
                    f"You have skipped {skipped_str}, which are required dependencies for other included resource types. "
                    f"This may cause the deployment to fail. "
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
                    crud_cls=step,
                    files=files_by_crud[step],
                )
            )
        return plan

    def _display_plan(self, client: ToolkitClient, plan: list[DeploymentStep]) -> None:
        raise NotImplementedError()

    def _apply_plan(
        self,
        client: ToolkitClient,
        env_vars: EnvironmentVariables,
        plan: list[DeploymentStep],
        dry_run: bool,
        force_update: bool,
    ) -> Sequence[DeploymentResult]:
        results: list[DeploymentResult] = []
        missing_write_acls: set[type[ResourceCRUD]] = set()
        with Progress() as progress:
            task_id = progress.add_task("Starting deploying", total=len(plan))
            for step in plan:
                crud = step.crud_cls.create_loader(client)
                resource_name = crud.display_name

                resources_by_id = self._load_resources(crud, step.files)
                resource_count = len(resources_by_id)

                missing_write_acl = self._validate_access(crud, list(resources_by_id.values()))
                missing_write_acls.update(missing_write_acl)
                progress.update(task_id, description=f"Comparing {resource_count} {resource_name} to CDF")
                cdf_resource_by_id = {
                    resource.as_id(): resource for resource in crud.retrieve(list(resources_by_id.keys()))
                }
                resources_to_deploy = self._categorize_resources(
                    resources_by_id, cdf_resource_by_id, force_update, dry_run
                )

                progress.update(task_id, description=f"Deploying {resource_name} to CDF")
                result = self.deploy_resources(crud, resources_to_deploy)

                progress.update(task_id, description=f"Deployed {resource_name} successfully.", advance=1)
                results.append(result)
            progress.update(task_id, description="Finished deploying.")
        return results

    def _load_resources(
        self, crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource], files: list[Path]
    ) -> dict[T_Identifier, T_RequestResource]:
        raise NotImplementedError()

    def _validate_access(
        self,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: list[T_RequestResource],
    ) -> Iterable[type[ResourceCRUD]]:
        raise NotImplementedError()

    def _categorize_resources(
        self,
        local_by_id: dict[T_Identifier, T_RequestResource],
        cdf_b_id: dict[T_Identifier, T_ResponseResource],
        force_update: bool,
        verbose: bool,
    ) -> Any:
        raise NotImplementedError()

    def deploy_resources(
        self, crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource], resources_to_deploy: Any
    ) -> DeploymentResult:
        raise NotImplementedError()

    def _display_results(self, results: Sequence[DeploymentResult]) -> None:
        raise NotImplementedError()
