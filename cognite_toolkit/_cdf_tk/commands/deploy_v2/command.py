import json
from collections import defaultdict
from collections.abc import Iterable, Sequence, Set
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Generic, Literal

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from yaml import YAMLError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildLineage
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT
from cognite_toolkit._cdf_tk.cruds import (
    RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND,
    ResourceContainerCRUD,
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ResourceDeleteError,
    ResourceUpdateError,
    ToolkitError,
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
from cognite_toolkit._cdf_tk.utils import humanize_collection, sanitize_filename, to_diff
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._version import __version__


@dataclass
class DeployOptions:
    cdf_project: str | None = None
    dry_run: bool = False
    include: Sequence[str] | None = None
    force_update: bool = False
    verbose: bool = False
    drop: bool = False
    drop_data: bool = False
    environment_variables: dict[str, str | None] | None = None
    deployment_dir: Path | None = None


@dataclass
class ResourceDirectory:
    directory: Path
    files_by_crud: dict[type[ResourceCRUD], list[Path]] = field(default_factory=lambda: defaultdict(list))
    invalid_files: list[Path] = field(default_factory=list)


@dataclass
class ReadBuildDirectory:
    path: Path
    resource_directories: list[ResourceDirectory] = field(default_factory=list)
    skipped_directories: list[ResourceDirectory] = field(default_factory=list)
    invalid_directories: list[Path] = field(default_factory=list)
    is_strict_validation: bool = False
    cdf_project: str | None = None

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
class ReadResource(Generic[T_RequestResource]):
    request: T_RequestResource
    raw_dict: dict[str, Any]
    source_files: list[Path] = field(default_factory=list)
    missing_env_vars: set[str] = field(default_factory=set)


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
    skipped_cruds: Set[type[ResourceCRUD]] = field(default_factory=set)


@dataclass
class Skipped(Generic[T_Identifier]):
    id: T_Identifier
    code: str
    source_file: Path
    reason: str


@dataclass
class ResourceToDeploy(Generic[T_Identifier, T_RequestResource]):
    to_create: list[T_RequestResource] = field(default_factory=list)
    to_delete: list[T_Identifier] = field(default_factory=list)
    to_update: list[T_RequestResource] = field(default_factory=list)
    unchanged: list[T_Identifier] = field(default_factory=list)
    skipped: list[Skipped[T_Identifier]] = field(default_factory=list)
    missing_env_vars_by_id: dict[T_Identifier, set[str]] = field(default_factory=dict)

    def get_ids(
        self,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        action: Literal["create", "delete", "update"],
    ) -> list[T_Identifier]:
        if action == "create":
            return [crud.get_id(resource) for resource in self.to_create]
        elif action == "delete":
            return self.to_delete
        elif action == "update":
            return [crud.get_id(resource) for resource in self.to_update]
        else:
            return []


@dataclass
class DeploymentResult:
    resource_name: str
    is_dry_run: bool
    created_count: int
    deleted_count: int
    updated_count: int
    unchanged_count: int
    is_missing_write_acl: bool
    skipped: list[Skipped] = field(default_factory=list)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)


class DeployV2Command(ToolkitCommand):
    def deploy(
        self,
        env_vars: EnvironmentVariables,
        user_build_dir: Path,
        options: DeployOptions | None = None,
    ) -> Sequence[DeploymentResult]:
        options = options or DeployOptions(environment_variables=env_vars.dump())
        build_dir = self.read_build_directory(user_build_dir, options.include)

        client = env_vars.get_client(is_strict_validation=build_dir.is_strict_validation)

        self._validate_cdf_project(build_dir, options.cdf_project, env_vars.CDF_PROJECT, client.console)
        self._display_startup(build_dir.path, client.config.project, client.console)
        self._display_read_dir(build_dir, client.console, options.verbose)

        plan = self.create_deployment_plan(build_dir)

        self._display_plan(plan, client.console)

        results = self.apply_plan(client, plan, options)

        # Todo: Some mixpanel tracking??
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
        # Note we support running without linage. This is for example used when deploying resources
        # with the upload command.from
        cdf_project: str | None = None
        if (lineage_path := (build_dir / BuildLineage.filename)).exists():
            lineage = BuildLineage.from_yaml_file(lineage_path)
            lineage.validate_source_files_unchanged()
            cdf_project = lineage.cdf_project
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
                    if yaml_file.stem.casefold().endswith(kind.casefold()):
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
            path=build_dir,
            resource_directories=resource_directories,
            invalid_directories=invalid_resource_dirs,
            skipped_directories=skipped_resource_dirs,
            cdf_project=cdf_project,
        )

    def _display_startup(self, build_dir: Path, cdf_project: str, console: Console) -> None:
        console.print(
            Panel(
                f"Deploying {build_dir.as_posix()} directory:\n  - Toolkit Version '{__version__!s}'\n"
                f"  - CDF project {cdf_project!r}",
                expand=False,
            )
        )

    def _display_read_dir(self, build_dir: ReadBuildDirectory, console: Console, verbose: bool) -> None:
        warnings = list(build_dir.create_warnings())
        resource_dir_count = len(build_dir.resource_directories)
        skipped_dir_count = len(build_dir.skipped_directories)
        invalid_dir_count = len(build_dir.invalid_directories)

        resource_file_count = sum(
            len(files) for dir_ in build_dir.resource_directories for files in dir_.files_by_crud.values()
        )
        invalid_yaml_file_count = sum(len(dir_.invalid_files) for dir_ in build_dir.resource_directories)

        lines = [
            f"Read {build_dir.path.as_posix()} complete.",
            f" - Found {resource_dir_count} resource directories",
            f" - Found {resource_file_count:,} resource files",
        ]
        if warnings:
            lines.append(f" - Found {len(warnings)} warnings during reading process")
        if skipped_dir_count:
            lines.append(f" - Found {skipped_dir_count} skipped directories")
        if invalid_dir_count:
            lines.append(f" - Found {invalid_dir_count} invalid directories")
        if invalid_yaml_file_count:
            lines.append(f" - Found {invalid_yaml_file_count} invalid yaml files")

        console.print("\n".join(lines))
        if warnings:
            for warning in build_dir.create_warnings():
                self.warn(warning, console=console)

        if (not verbose and skipped_dir_count) or invalid_dir_count or invalid_yaml_file_count:
            console.print(
                f"{HINT_LEAD_TEXT} Use --verbose flag to get more details about the skipped and invalid directories and files."
            )
        if verbose:
            if build_dir.skipped_directories:
                skipped_str = "\n".join([dir_.directory.as_posix() for dir_ in build_dir.skipped_directories])
                console.print(f"Skipped directories:\n{skipped_str}")
            if build_dir.invalid_directories:
                invalid_str = "\n".join([skipped.as_posix() for skipped in build_dir.invalid_directories])
                console.print(f"Invalid directories\n{invalid_str}")
            if invalid_yaml_file_count:
                invalid_str = "\n".join(
                    [file.as_posix() for dir_ in build_dir.resource_directories for file in dir_.invalid_files]
                )
                console.print(f"Invalid yaml files\n{invalid_str}")

    def _validate_cdf_project(
        self, build_dir: ReadBuildDirectory, cli_cdf_project: str | None, client_cdf_project: str, console: Console
    ) -> None:
        """Validates that the user is deploying to the CDF project they intended"""
        if cli_cdf_project is not None and cli_cdf_project != client_cdf_project:
            raise ToolkitValidationError(
                f"The CDF project in your command argument does not match your credentials, "
                f"{cli_cdf_project!r}≠{client_cdf_project!r}."
            )
        elif (
            cli_cdf_project is None
            and build_dir.cdf_project is not None
            and build_dir.cdf_project != client_cdf_project
        ):
            raise ToolkitValidationError(
                f"The configurations were built for the {build_dir.cdf_project!r} CDF project, but your credentials are for {client_cdf_project!r}, "
                f"{build_dir.cdf_project!r}≠{client_cdf_project!r}."
            )
        elif cli_cdf_project is None and build_dir.cdf_project is None:
            typed_project = questionary.text(
                f"Enter the name of CDF project you are deploying to. This must match the CDF_PROJECT={client_cdf_project!r} in you environment variables.\n",
            ).unsafe_ask()
            if typed_project != client_cdf_project:
                raise ToolkitValidationError(
                    f"The CDF project you typed does not match your credentials, "
                    f"{typed_project!r}≠{client_cdf_project!r}."
                )

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
    def _display_plan(cls, plan: list[DeploymentStep], console: Console) -> None:
        if not plan:
            console.print("[bold yellow]No resources to deploy.[/]")
            return

        step_count = len(plan)
        total_files = sum(len(step.files) for step in plan)

        lines = [
            "Deployment plan",
            f" - {step_count} resource types to deploy",
            f" - {total_files} resources to deploy",
        ]
        console.print(*lines, sep="\n")

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
            total_files = sum(len(step.files) for step in plan)
            task_id = progress.add_task("Starting deployment", total=total_files)
            for step in plan:
                crud = step.crud_cls.create_loader(client)
                resource_name = crud.display_name
                progress.update(task_id, description=f"Reading {resource_name}")

                resource_by_id = cls._read_resource_files(crud, step.files, options)
                resource_count = len(resource_by_id)
                request_resources = [resource.request for resource in resource_by_id.values()]

                is_missing_write = cls._validate_access(crud, request_resources, is_dry_run=options.dry_run)

                progress.update(task_id, description=f"Comparing {resource_count} {resource_name} to CDF")
                cdf_resource_by_id = {
                    crud.get_id(resource): resource for resource in crud.retrieve(list(resource_by_id.keys()))
                }
                resources_to_deploy = cls._categorize_resources(
                    crud,
                    resource_by_id,
                    cdf_resource_by_id,
                    console,
                    options,
                )

                if options.dry_run:
                    result = cls.deploy_dry_run(crud, resources_to_deploy, is_missing_write, options)
                    progress.update(task_id, description=f"Would have deployed {resource_name} to CDF")
                else:
                    progress.update(task_id, description=f"Deploying {resource_name} to CDF")
                    result = cls.deploy_resources(crud, resources_to_deploy, step.skipped_cruds, options.deployment_dir)
                    progress.update(task_id, description=f"Deployed {resource_name} successfully.")

                results.append(result)

                progress.update(task_id, advance=len(step.files))
            progress.update(task_id, description="Finished deploying.")
        return results

    @classmethod
    def _read_resource_files(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        filepaths: list[Path],
        options: DeployOptions,
    ) -> dict[T_Identifier, ReadResource[T_RequestResource]]:
        """# Load all resources from files, get ids, and remove duplicates."""
        to_deploy_by_id: dict[T_Identifier, ReadResource[T_RequestResource]] = {}
        environment_variables = options.environment_variables or {}
        for filepath in filepaths:
            with catch_warnings(EnvironmentVariableMissingWarning) as warning_list:
                try:
                    resource_list = crud.load_resource_file(filepath, environment_variables)
                except YAMLError as e:
                    raise ToolkitYAMLFormatError(f"YAML validation error for {filepath.as_posix()}: {e}")
            # Note we only catch EnvironmentVariableMissingWarning, so all warnings should be of that type.
            missing_env_vars = {
                env_var
                for warning in warning_list
                if isinstance(warning, EnvironmentVariableMissingWarning)
                for env_var in warning.variables
            }
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

                if identifier not in to_deploy_by_id:
                    to_deploy_by_id[identifier] = ReadResource(
                        request=request_resource,
                        raw_dict=resource_dict,
                        source_files=[filepath],
                        missing_env_vars=missing_env_vars,
                    )
                else:
                    to_deploy_by_id[identifier].source_files.append(filepath)

        return to_deploy_by_id

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
        resource_by_id: dict[T_Identifier, ReadResource[T_RequestResource]],
        cdf_by_id: dict[T_Identifier, T_ResponseResource],
        console: Console,
        options: DeployOptions,
    ) -> ResourceToDeploy:
        resources = ResourceToDeploy[T_Identifier, T_RequestResource]()
        for identifier, resource in resource_by_id.items():
            if len(resource.source_files) > 1:
                first_file = resource.source_files[0]
                for filepath in resource.source_files[1:]:
                    resources.skipped.append(
                        Skipped(
                            identifier,
                            "AMBIGUOUS",
                            filepath,
                            f"Identifier is not unique. Will use definition in {first_file.as_posix()}",
                        )
                    )
            # Persist as this is used in the deploy context.
            resources.missing_env_vars_by_id[identifier] = resource.missing_env_vars

            cdf_resource = cdf_by_id.get(identifier)
            if cdf_resource is None:
                resources.to_create.append(resource.request)
                continue
            cdf_dict = crud.dump_resource(cdf_resource, resource.raw_dict)
            if not options.force_update and cdf_dict == resource.raw_dict:
                resources.unchanged.append(identifier)
                continue
            if crud.support_update:
                resources.to_update.append(resource.request)
            else:
                resources.to_delete.append(identifier)
                resources.to_create.append(resource.request)
            if options.verbose:
                diff_str = "\n".join(to_diff(cdf_dict, resource.raw_dict))
                for sensitive in crud.sensitive_strings(resource.request):
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
            created_count=created,
            updated_count=updated,
            deleted_count=deleted,
            unchanged_count=unchanged,
            skipped=resources.skipped,
            is_missing_write_acl=is_missing_write_acl,
        )

    @classmethod
    def deploy_resources(
        cls,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        skipped_cruds: Set[type[ResourceCRUD]],
        deploy_dir: Path | None = None,
    ) -> DeploymentResult:
        deleted, created, updated = 0, 0, 0
        action: Literal["create", "delete", "update"] | None = None
        try:
            if resources.to_delete:
                action = "delete"
                deleted = crud.delete(resources.to_delete)
            if resources.to_create:
                action = "create"
                created = len(crud.create(resources.to_create))
            if resources.to_update:
                action = "update"
                updated = len(crud.update(resources.to_update))
        except ToolkitAPIError as error:
            cls._handle_deploy_error(error, action, crud, resources, skipped_cruds, deploy_dir)

        return DeploymentResult(
            resource_name=crud.display_name,
            is_dry_run=False,
            created_count=created,
            updated_count=updated,
            deleted_count=deleted,
            unchanged_count=len(resources.unchanged),
            skipped=resources.skipped,
            is_missing_write_acl=False,
        )

    @classmethod
    def _handle_deploy_error(
        cls,
        error: ToolkitAPIError,
        action: Literal["create", "delete", "update"] | None,
        crud: ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        skipped_cruds: Set[type[ResourceCRUD]],
        deploy_dir: Path | None = None,
    ) -> None:
        if action is None:
            raise RuntimeError("Bug in Toolkit. No action to perform but got API error.") from error
        if error_message := cls._missing_environment_variables(crud, resources, action):
            raise cls._get_resource_exception(action)(error_message) from error

        suffix = ""
        if deploy_dir:
            filepath = deploy_dir / f"{sanitize_filename(datetime.now(timezone.utc).isoformat())}.json"
            suffix = (
                f"\nThe request body and response has been written to {filepath.as_posix()} for debugging purposes."
            )
            json_str = json.dumps(error.as_debug_dict(), indent=2, sort_keys=False)
            for item in resources.to_create + resources.to_update:
                for string in crud.sensitive_strings(item):
                    json_str = json_str.replace(string, "********")
            filepath.write_text(json_str, encoding="utf-8")

        if skipped_cruds:
            error_message = (
                f"Failed to {action} {crud.display_name}. This is likely due to missing dependencies on "
                f"{humanize_collection([crud.display_name for crud in skipped_cruds])} which were "
                f"skipped based on the include filter.{suffix}"
            )
        else:
            error_message = f"Failed to {action} {crud.display_name} due to API error: {error.message}.{suffix}"
        raise cls._get_resource_exception(action)(error_message) from error

    @classmethod
    def _missing_environment_variables(
        cls, crud: ResourceCRUD, resources: ResourceToDeploy, action: Literal["create", "delete", "update"]
    ) -> str | None:
        item_ids = resources.get_ids(crud, action)
        match = set(item_ids) & set(resources.missing_env_vars_by_id)
        if not match:
            return None
        missing_variables = [variable for id in match for variable in resources.missing_env_vars_by_id[id]]
        variables_str = humanize_collection(missing_variables)
        suffix = "s" if len(missing_variables) > 1 else ""
        return f"\n  {HINT_LEAD_TEXT}This is likely due to missing environment variable{suffix}: {variables_str}"

    @classmethod
    def _get_resource_exception(cls, action: Literal["create", "update", "delete"]) -> type[ToolkitError]:
        return {"update": ResourceUpdateError, "delete": ResourceDeleteError, "create": ResourceCreationError}[action]

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
                str(result.created_count),
                str(result.updated_count),
                str(result.deleted_count),
                str(result.unchanged_count),
            )
            total_created += result.created_count
            total_updated += result.updated_count
            total_deleted += result.deleted_count
            total_unchanged += result.unchanged_count

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
