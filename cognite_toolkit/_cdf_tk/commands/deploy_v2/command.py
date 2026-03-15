import warnings
from collections import defaultdict
from collections.abc import Iterable, Sequence, Set
from copy import deepcopy
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Generic

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from yaml import YAMLError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND,
    ResourceContainerCRUD,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitWrongResourceError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    EnvironmentVariableMissingWarning,
    LowSeverityWarning,
    ToolkitWarning,
    catch_warnings,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_diff
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


@dataclass
class DeployOptions:
    dry_run: bool = False
    include: Sequence[str] | None = None
    force_update: bool = False
    verbose: bool = False
    drop: bool = False
    drop_data: bool = False
    environment_variables: dict[str, str | None] | None = None


@dataclass
class ResourceDirectory:
    directory: Path
    files_by_crud: dict[type[ResourceCRUD], list[Path]] = field(default_factory=lambda: defaultdict(list))
    invalid_files: list[Path] = field(default_factory=list)


@dataclass
class ReadBuildDirectory:
    build_dir: Path
    resource_directories: list[ResourceDirectory] = field(default_factory=list)
    skipped_directories: list[ResourceDirectory] = field(default_factory=list)
    invalid_directories: list[Path] = field(default_factory=list)
    is_strict_validation: bool = False

    def create_warnings(self) -> Iterable[ToolkitWarning]:
        for invalid_dir in self.invalid_directories:
            yield LowSeverityWarning(f"Unrecognized resource directory {invalid_dir.name!r} in build output, skipping.")
        for resource_dir in self.resource_directories:
            for invalid_file in resource_dir.invalid_files:
                yield LowSeverityWarning(
                    f"File {invalid_file.name!r} in {resource_dir.directory.name!r} does not match any known resource kind, skipping."
                )

    def skipped_cruds(self) -> set[type[ResourceCRUD]]:
        return {crud for dir in self.skipped_directories for crud in dir.files_by_crud.keys()}

    def as_files_by_crud(self) -> dict[type[ResourceCRUD], list[Path]]:
        files_by_crud: dict[type[ResourceCRUD], list[Path]] = {}
        for dir in self.resource_directories:
            files_by_crud.update(dir.files_by_crud)
        return files_by_crud


@dataclass
class DeploymentStep:
    """A deployment step

    Args:
        crud_cls: The CRUD class to use for this step.
        files: The files to deploy in this step, all of which should be of the same structure.
        skipped_cruds: Resource types that this step depends on but are skipped due to the include filter.
            This is used to warn the user about potential issues with the deployment.

    """

    crud_cls: type[ResourceCRUD]
    files: list[Path]
    # Todo: Warn about skipped CRUDs. Maybe only if deployment fails?
    skipped_cruds: Set[type[ResourceCRUD]] = field(default_factory=set)


@dataclass
class Skipped(Generic[T_Identifier]):
    id: T_Identifier
    source_file: Path
    reason: str


@dataclass
class ResourceToDeploy(Generic[T_Identifier, T_RequestResource]):
    to_create: list[T_RequestResource] = field(default_factory=list)
    to_delete: list[T_Identifier] = field(default_factory=list)
    to_update: list[T_RequestResource] = field(default_factory=list)
    unchanged: list[T_Identifier] = field(default_factory=list)
    skipped: list[Skipped[T_Identifier]] = field(default_factory=list)


@dataclass
class DeploymentResult:
    resource_name: str
    is_dry_run: bool
    created: int
    deleted: int
    updated: int
    unchanged: int
    skipped: int
    is_missing_write_acl: bool


class DeployV2Command(ToolkitCommand):
    def deploy(
        self,
        env_vars: EnvironmentVariables,
        build_dir: Path,
        options: DeployOptions | None = None,
    ) -> Sequence[DeploymentResult]:
        options = options or DeployOptions(environment_variables=env_vars.dump())
        read_dir = self.read_build_directory(build_dir, options.include)

        client = env_vars.get_client(is_strict_validation=read_dir.is_strict_validation)

        self._display_read_dir(read_dir, client.console)

        plan = self.create_deployment_plan(read_dir)

        self._display_plan(client, plan)

        results = self.apply_plan(client, plan, options)

        self._display_results(client, results)

        return results

    @classmethod
    def read_build_directory(cls, build_dir: Path, include: Sequence[str] | None = None) -> ReadBuildDirectory:
        """Reads the build directory and returns a structured representation of the resources to be deployed.

        Args:
            build_dir: The build directory to read.
            include: The include filter to apply to the resources to deploy

        Returns:
            A ReadBuildDirectory object containing the structured representation of the resources to be deployed,
            as well as any warnings or errors encountered during the reading process.
        """
        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"Build directory {build_dir!s} does not exist.")
        available_resource_types = set(RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND.keys())
        if include and (invalid := set(include) - available_resource_types):
            raise ToolkitValidationError(
                f"Invalid resource types specified: {humanize_collection(invalid)}, available types: {humanize_collection(available_resource_types)}"
            )
        # Todo: Check linage file.
        #   - Check source hash are unchanged
        #   - Check that CDF Project matches env.
        #   - If linage file is missing, ask user to type in the CDF Project they are
        #     writing to.
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

    def _display_read_dir(self, read_dir: ReadBuildDirectory, console: Console) -> None:
        console.print(f"Read {read_dir.build_dir.as_posix()} complete")
        warnings = list(read_dir.create_warnings())
        if warnings:
            console.print(f"Found {len(warnings)} warnings")
            for warning in read_dir.create_warnings():
                self.warn(warning, console=console)

    @classmethod
    def create_deployment_plan(cls, read_dir: ReadBuildDirectory) -> list[DeploymentStep]:
        """Creates the deployment plan for the build directory.

        This function topological sorts the resource types based on their dependencies on each other.

        Args:
            read_dir: The structured representation of the build directory.

        Returns:
            A list of DeploymentStep objects representing the deployment plan.
        """
        files_by_crud = read_dir.as_files_by_crud()
        skipped_cruds = read_dir.skipped_cruds()
        dependencies_by_crud: dict[type[ResourceCRUD], Set[type[ResourceCRUD]]] = {}
        skipped_by_crud: dict[type[ResourceCRUD], Set[type[ResourceCRUD]]] = {}
        for crud_cls in files_by_crud.keys():
            dependencies = crud_cls.dependencies
            if missing := (skipped_cruds.intersection(dependencies)):
                skipped_by_crud[crud_cls] = missing
            dependencies_by_crud[crud_cls] = dependencies

        try:
            ordered = list(TopologicalSorter(dependencies_by_crud).static_order())
        except CycleError as e:
            raise RuntimeError("Bug in Toolkit. Cyclic dependencies in support resource types detected.") from e

        plan: list[DeploymentStep] = []
        for step in ordered:
            if step not in files_by_crud:
                continue
            plan.append(
                DeploymentStep(crud_cls=step, files=files_by_crud[step], skipped_cruds=skipped_by_crud.get(step, set()))
            )
        return plan

    @classmethod
    def _display_plan(cls, client: ToolkitClient, plan: list[DeploymentStep]) -> None:
        if not plan:
            client.console.print("[bold yellow]No resources to deploy.[/]")
            return
        table = Table(title="Deployment Plan", show_lines=False)
        table.add_column("#", style="dim", width=4)
        table.add_column("Resource Type", style="cyan")
        table.add_column("Files", justify="right")
        for i, step in enumerate(plan, 1):
            crud_name = step.crud_cls.folder_name
            table.add_row(str(i), crud_name, str(len(step.files)))
        client.console.print(table)

    @classmethod
    def apply_plan(
        cls, client: ToolkitClient, plan: list[DeploymentStep], options: DeployOptions
    ) -> Sequence[DeploymentResult]:
        """Applies the given plan using the given client.

        Args:
            client: The client to use to apply the plan.
            plan: The plan to apply.
            options: The options to use when applying the plan.

        Returns:
            A list of DeploymentResult objects matching the given plan.
        """

        results: list[DeploymentResult] = []
        console = client.console
        with Progress(console=console) as progress:
            task_id = progress.add_task("Starting deploying", total=len(plan))
            for step in plan:
                crud = step.crud_cls.create_loader(client)
                resource_name = crud.display_name
                progress.update(task_id, description=f"Reading {resource_name}")

                resources_by_id, source_files = cls._read_resource_files(crud, step.files, console, options)
                resource_count = len(resources_by_id)
                request_resources = [resource for _, resource in resources_by_id.values()]

                is_missing_write = cls._validate_access(crud, request_resources, is_dry_run=options.dry_run)

                progress.update(task_id, description=f"Comparing {resource_count} {resource_name} to CDF")
                cdf_resource_by_id = {
                    crud.get_id(resource): resource for resource in crud.retrieve(list(resources_by_id.keys()))
                }
                resources_to_deploy = cls._categorize_resources(
                    crud,
                    resources_by_id,
                    cdf_resource_by_id,
                    source_files,
                    console,
                    options,
                )

                if options.dry_run:
                    result = cls.deploy_dry_run(crud, resources_to_deploy, is_missing_write, options)
                    progress.update(task_id, description=f"Would have deployed {resource_name} to CDF")
                else:
                    progress.update(task_id, description=f"Deploying {resource_name} to CDF")
                    result = cls.deploy_resources(crud, resources_to_deploy)
                    progress.update(task_id, description=f"Deployed {resource_name} successfully.")

                results.append(result)

                progress.update(task_id, advance=1)
            progress.update(task_id, description="Finished deploying.")
        return results

    @classmethod
    def _read_resource_files(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        filepaths: list[Path],
        console: Console,
        options: DeployOptions,
    ) -> tuple[dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]], dict[T_Identifier, list[Path]]]:
        """# Load all resources from files, get ids, and remove duplicates."""
        local_by_id: dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]] = {}
        environment_variables = options.environment_variables or {}
        source_file_by_ids: dict[T_Identifier, list[Path]] = defaultdict(list)
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
                    request_resource = crud.load_resource(deepcopy(resource_dict), options.dry_run)
                except ToolkitWrongResourceError:
                    # The ToolkitWrongResourceError is a special exception that as of 21/12/24 is used by
                    # the GroupAllScopedLoader and GroupResourceScopedLoader to signal that the resource
                    # should be handled by the other loader.
                    continue
                identifier = crud.get_id(request_resource)

                source_file_by_ids[identifier].append(filepath)
                if identifier not in local_by_id:
                    local_by_id[identifier] = resource_dict, request_resource

            for warning in warning_list:
                if isinstance(warning, EnvironmentVariableMissingWarning):
                    # Warnings are immutable, so we use the below method to override it.
                    object.__setattr__(warning, "identifiers", frozenset(identifiers))
                    # Reraise the warning to be caught higher up.
                    warnings.warn(warning, stacklevel=2)
                else:
                    warning.print_warning(console=console)
        return local_by_id, source_file_by_ids

    @classmethod
    def _validate_access(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: list[T_RequestResource],
        is_dry_run: bool,
    ) -> bool:
        minimum_scope = crud.get_minimum_scope(resources)
        if minimum_scope is None:
            return False
        if is_dry_run:
            required_acls = list(crud.create_acl({"READ"}, minimum_scope))
            optional_acls = list(crud.create_acl({"WRITE"}, minimum_scope))
        else:
            required_acls = list(crud.create_acl({"READ", "WRITE"}, minimum_scope))
            optional_acls = []

        if missing := crud.client.tool.token.verify_acls(required_acls):
            raise crud.client.tool.token.create_error(missing, action=f"deploy {crud.display_name}")

        return bool(crud.client.tool.token.verify_acls(optional_acls))

    @classmethod
    def _categorize_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        local_by_id: dict[T_Identifier, tuple[dict[str, Any], T_RequestResource]],
        cdf_by_id: dict[T_Identifier, T_ResponseResource],
        source_files: dict[T_Identifier, list[Path]],
        console: Console,
        options: DeployOptions,
    ) -> ResourceToDeploy:
        resources = ResourceToDeploy[T_Identifier, T_RequestResource]()
        for identifier, (local_dict, local_resource) in local_by_id.items():
            if len(source_files[identifier]) > 1:
                first_file = source_files[identifier][0]
                for filepath in source_files[identifier][1:]:
                    resources.skipped.append(
                        Skipped(
                            identifier, filepath, f"Duplicated resource. Will use definition in {first_file.as_posix()}"
                        )
                    )

            cdf_resource = cdf_by_id.get(identifier)
            if cdf_resource is None:
                resources.to_create.append(local_resource)
                continue
            cdf_dict = crud.dump_resource(cdf_resource, local_dict)
            if not options.force_update and cdf_dict == local_dict:
                resources.unchanged.append(identifier)
                continue
            if crud.support_update:
                resources.to_update.append(local_resource)
            else:
                resources.to_delete.append(identifier)
                resources.to_create.append(local_resource)
            if options.verbose:
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
    def deploy_dry_run(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        is_missing_write_acl: bool,
        options: DeployOptions,
    ) -> DeploymentResult:
        created = len(resources.to_create)
        updated = len(resources.to_update)
        deleted = len(resources.to_delete)
        unchanged = len(resources.unchanged)

        is_container = isinstance(crud, ResourceContainerCRUD)
        if options.drop and crud.support_drop and (not is_container or options.drop_data):
            # If drop/drop_data arguments are passed, then we will delete and recreate resources.
            created += unchanged + updated
            deleted += unchanged + updated
            unchanged = 0
            updated = 0

        return DeploymentResult(
            resource_name=crud.display_name,
            is_dry_run=True,
            created=created,
            updated=updated,
            deleted=deleted,
            unchanged=unchanged,
            skipped=len(resources.skipped),
            is_missing_write_acl=is_missing_write_acl,
        )

    @classmethod
    def deploy_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
    ) -> DeploymentResult:
        deleted, created, updated = 0, 0, 0
        # Todo: Handle API errors.
        if resources.to_delete:
            deleted = crud.delete(resources.to_delete)
        if resources.to_create:
            created = len(crud.create(resources.to_create))
        if resources.to_update:
            updated = len(crud.update(resources.to_update))
        return DeploymentResult(
            resource_name=crud.display_name,
            is_dry_run=False,
            created=created,
            updated=updated,
            deleted=deleted,
            unchanged=len(resources.unchanged),
            skipped=len(resources.skipped),
            is_missing_write_acl=False,
        )

    @classmethod
    def _display_results(cls, client: ToolkitClient, results: Sequence[DeploymentResult]) -> None:
        if not results:
            client.console.print("No resources were deployed.")
            return

        is_dry_run = results[0].is_dry_run
        title = "Deployment Summary (dry run)" if is_dry_run else "Deployment Summary"
        table = Table(title=title, show_lines=False)
        table.add_column("Resource", style="cyan")
        table.add_column("Created", justify="right", style="green")
        table.add_column("Updated", justify="right", style="yellow")
        table.add_column("Deleted", justify="right", style="red")
        table.add_column("Unchanged", justify="right", style="dim")

        total_created, total_updated, total_deleted, total_unchanged = 0, 0, 0, 0
        for result in results:
            table.add_row(
                result.resource_name,
                str(result.created),
                str(result.updated),
                str(result.deleted),
                str(result.unchanged),
            )
            total_created += result.created
            total_updated += result.updated
            total_deleted += result.deleted
            total_unchanged += result.unchanged

        if len(results) > 1:
            table.add_section()
            table.add_row(
                "[bold]Total[/]",
                f"[bold]{total_created}[/]",
                f"[bold]{total_updated}[/]",
                f"[bold]{total_deleted}[/]",
                f"[bold]{total_unchanged}[/]",
            )

        client.console.print(table)
