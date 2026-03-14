import warnings
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence, Set
from copy import deepcopy
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Generic, Literal

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from yaml import YAMLError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitWrongResourceError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning, ToolkitWarning, catch_warnings
from cognite_toolkit._cdf_tk.tk_warnings.other import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_diff
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
        # Todo: Implement
        yield from ()

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
    to_create: list[T_RequestResource] = field(default_factory=list)
    to_delete: list[T_Identifier] = field(default_factory=list)
    to_update: list[T_RequestResource] = field(default_factory=list)
    unchanged: list[T_Identifier] = field(default_factory=list)


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
        options = options or DeployOptions()
        read_dir = self._read_build_directory(build_dir, options.include)

        client = env_vars.get_client(is_strict_validation=read_dir.is_strict_validation)

        for warning in read_dir.create_warnings():
            self.warn(warning, console=client.console)

        plan = self._create_deployment_plan(read_dir)

        self._display_plan(client, plan)

        results = self._apply_plan(client, plan, options.dry_run, options.force_update, env_vars.dump())

        self._display_results(results)

        return results

    @classmethod
    def _read_build_directory(cls, build_dir: Path, include: Sequence[str] | None = None) -> ReadBuildDirectory:
        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"Build directory {build_dir!s} does not exist.")
        available_resource_types = set(RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND.keys())
        if include:
            if invalid := set(include) - available_resource_types:
                raise ToolkitValidationError(
                    f"Invalid resource types specified: {humanize_collection(invalid)}, available types: {humanize_collection(available_resource_types)}"
                )
        # Todo: Check linage file.
        include_set = set(include) if include else None
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
            if include_set is not None and resource_dir.name not in include_set:
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

    @classmethod
    def _display_plan(cls, client: ToolkitClient, plan: list[DeploymentStep]) -> None:
        # Todo: Implement
        return

    @classmethod
    def _apply_plan(
        cls,
        client: ToolkitClient,
        plan: list[DeploymentStep],
        dry_run: bool,
        force_update: bool,
        environment_variables: dict[str, str | None] | None,
    ) -> Sequence[DeploymentResult]:
        results: list[DeploymentResult] = []
        missing_write_acls: set[str] = set()
        with Progress(console=client.console) as progress:
            task_id = progress.add_task("Starting deploying", total=len(plan))
            for step in plan:
                crud = step.crud_cls.create_loader(client)
                resource_name = crud.display_name
                progress.update(task_id, description=f"Reading {resource_name}")

                resources_by_id = cls._load_resources(
                    crud, step.files, is_dry_run=dry_run, environment_variables=environment_variables
                )
                resource_count = len(resources_by_id)

                missing_write_acl = cls._validate_access(
                    crud, [resource for _, resource in resources_by_id.values()], is_dry_run=dry_run
                )
                missing_write_acls.update(missing_write_acl)
                progress.update(task_id, description=f"Comparing {resource_count} {resource_name} to CDF")

                cdf_resource_by_id = {
                    resource.as_id(): resource for resource in crud.retrieve(list(resources_by_id.keys()))
                }

                resources_to_deploy = cls._categorize_resources(
                    crud, resources_by_id, cdf_resource_by_id, force_update, dry_run, client.console
                )

                if dry_run:
                    result = cls.deploy_dry_run(resources_to_deploy)
                    results.append(result)
                    progress.update(task_id, description=f"Would have deployed {resource_name} to CDF")
                else:
                    progress.update(task_id, description=f"Deploying {resource_name} to CDF")
                    result = cls.deploy_resources(crud, resources_to_deploy)
                    progress.update(task_id, description=f"Deployed {resource_name} successfully.")
                    results.append(result)

                progress.update(task_id, advance=1)
            progress.update(task_id, description="Finished deploying.")
            # Todo: What about missing write access? - Warn user that they will not be able to deploy.
        return results

    @classmethod
    def _load_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        filepaths: list[Path],
        is_dry_run: bool,
        environment_variables: dict[str, str | None] | None = None,
    ) -> dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]]:
        """# Load all resources from files, get ids, and remove duplicates."""
        local_by_id: dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]] = {}
        environment_variables = environment_variables or {}
        duplicates: Counter[T_Identifier] = Counter()
        for filepath in filepaths:
            with catch_warnings(EnvironmentVariableMissingWarning) as warning_list:
                try:
                    resource_list = crud.load_resource_file(filepath, environment_variables)
                except YAMLError as e:
                    raise ToolkitYAMLFormatError(f"YAML validation error for {filepath.as_posix()}: {e}")
            identifiers: list[T_Identifier] = []
            for resource_dict in resource_list:
                try:
                    # The load resource modifies the resource_dict, so we deepcopy it to avoid side effects.
                    request_resource = crud.load_resource(deepcopy(resource_dict), is_dry_run)
                except ToolkitWrongResourceError:
                    # The ToolkitWrongResourceError is a special exception that as of 21/12/24 is used by
                    # the GroupAllScopedLoader and GroupResourceScopedLoader to signal that the resource
                    # should be handled by the other loader.
                    continue
                identifier = crud.get_id(request_resource)
                if identifier in local_by_id:
                    duplicates.update([identifier])
                else:
                    local_by_id[identifier] = resource_dict, request_resource

            for warning in warning_list:
                if isinstance(warning, EnvironmentVariableMissingWarning):
                    # Warnings are immutable, so we use the below method to override it.
                    object.__setattr__(warning, "identifiers", frozenset(identifiers))
                    # Reraise the warning to be caught higher up.
                    warnings.warn(warning, stacklevel=2)
                else:
                    warning.print_warning()
        # Todo: What to do about duplicates? Count as skipped with reason.
        return local_by_id

    @classmethod
    def _validate_access(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: list[T_RequestResource],
        is_dry_run: bool,
    ) -> Iterable[str]:
        actions: set[Literal["READ", "WRITE"]] = {"READ"} if is_dry_run else {"READ", "WRITE"}
        required_acls = list(crud.create_minimum_acl(actions, resources))
        if missing := crud.client.tool.token.verify_acls(required_acls):
            raise crud.client.tool.token.create_error(missing, action=f"deploy {crud.display_name}")
        if is_dry_run and crud.client.tool.token.verify_acls(
            list(crud.create_minimum_acl({"READ", "WRITE"}, resources))
        ):
            yield crud.display_name

    @classmethod
    def _categorize_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        local_by_id: dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]],
        cdf_by_id: dict[T_Identifier, T_ResponseResource],
        force_update: bool,
        verbose: bool,
        console: Console,
    ) -> ResourceToDeploy:
        resources = ResourceToDeploy[T_Identifier, T_RequestResource]()
        for identifier, (local_dict, local_resource) in local_by_id.items():
            cdf_resource = cdf_by_id.get(identifier)
            if cdf_resource is None:
                resources.to_create.append(local_resource)
                continue
            cdf_dict = crud.dump_resource(cdf_resource, local_dict)
            if not force_update and cdf_dict == local_dict:
                resources.unchanged.append(identifier)
                continue
            if crud.support_update:
                resources.to_update.append(local_resource)
            else:
                resources.to_delete.append(identifier)
                resources.to_create.append(local_resource)
            if verbose:
                diff_str = "\n".join(to_diff(cdf_dict, local_dict))
                for sensitive in crud.sensitive_strings(local_resource):
                    diff_str = diff_str.replace(sensitive, "********")
                console.print(
                    Panel(
                        diff_str,
                        title=f"{crud.display_name}: {identifier}",
                        expand=False,
                    )
                )
        return resources

    @classmethod
    def deploy_dry_run(cls, resources: ResourceToDeploy[T_Identifier, T_RequestResource]) -> DeploymentResult:
        # Todo: Handle drop and drop-data.
        return DeploymentResult(
            is_dry_run=True,
            created=len(resources.to_create),
            updated=len(resources.to_update),
            deleted=len(resources.to_delete),
            unchanged=len(resources.unchanged),
        )

    @classmethod
    def deploy_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
    ) -> DeploymentResult:
        deleted, created, updated, unchanged = 0, 0, 0, len(resources.unchanged)
        if resources.to_delete:
            deleted = crud.delete(resources.to_delete)
        if resources.to_create:
            created = len(crud.create(resources.to_create))
        if resources.to_update:
            updated = len(crud.update(resources.to_update))
        return DeploymentResult(
            is_dry_run=False,
            created=created,
            updated=updated,
            deleted=deleted,
            unchanged=unchanged,
        )

    @classmethod
    def _display_results(cls, results: Sequence[DeploymentResult]) -> None:
        # Todo: implement
        return
