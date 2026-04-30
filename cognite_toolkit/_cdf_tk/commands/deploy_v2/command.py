import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Generic, Literal, TypeAlias

import questionary
from pydantic import ValidationError
from rich.console import Console, Group, RenderableType
from rich.markup import escape
from rich.padding import Padding
from rich.progress import Progress
from yaml import YAMLError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import RawTableId
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildLineage
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT
from cognite_toolkit._cdf_tk.data_classes._tracking_info import DeploymentTracking
from cognite_toolkit._cdf_tk.dataio.selectors import RawTableSelector, SelectedTable
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ResourceDeleteError,
    ResourceRetrievalError,
    ResourceUpdateError,
    ToolkitError,
    ToolkitNotADirectoryError,
    ToolkitValidationError,
    ToolkitValueError,
    ToolkitWrongResourceError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    RawTableCRUD,
    ResourceContainerIO,
    ResourceIO,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    EnvironmentVariableMissingWarning,
    LowSeverityWarning,
    ToolkitWarning,
    catch_warnings,
)
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.ui import AuraColor, ToolkitPanel, ToolkitPanelSection, ToolkitTable, hanging_indent
from cognite_toolkit._cdf_tk.utils import humanize_collection, sanitize_filename, to_diff
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._version import __version__

Operation: TypeAlias = Literal["deploy", "clean"]


@dataclass
class DeployOptions:
    operation: Operation = "deploy"
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
    files_by_crud: dict[type[ResourceIO], list[Path]] = field(default_factory=lambda: defaultdict(list))
    invalid_files: list[Path] = field(default_factory=list)
    extra_files: list[Path] = field(default_factory=list)


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

    def skipped_cruds(self) -> set[type[ResourceIO]]:
        return {crud for dir in self.skipped_directories for crud in dir.files_by_crud.keys()}

    def as_files_by_crud(self) -> dict[type[ResourceIO], list[Path]]:
        files_by_crud: dict[type[ResourceIO], list[Path]] = {}
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

    crud_cls: type[ResourceIO]
    files: list[Path]
    skipped_cruds: Set[type[ResourceIO]] = field(default_factory=set)


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
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
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

    @property
    def total_count(self) -> int:
        return self.created_count + self.deleted_count + self.updated_count + self.unchanged_count + self.skipped_count

    def __iadd__(self, other: "DeploymentResult") -> "DeploymentResult":
        self.created_count += other.created_count
        self.deleted_count += other.deleted_count
        self.updated_count += other.updated_count
        self.unchanged_count += other.unchanged_count
        self.is_missing_write_acl = self.is_missing_write_acl or other.is_missing_write_acl
        self.skipped.extend(other.skipped)
        return self


class DeployV2Command(ToolkitCommand):
    def deploy(
        self,
        env_vars: EnvironmentVariables,
        user_build_dir: Path,
        options: DeployOptions | None = None,
    ) -> Sequence[DeploymentResult]:
        options = options or DeployOptions(environment_variables=env_vars.dump())
        build_lineage = self.read_build_lineage(user_build_dir)
        build_dir = self.read_build_directory(user_build_dir, options.include, build_lineage)

        client = env_vars.get_client(is_strict_validation=build_dir.is_strict_validation)

        self._validate_cdf_project(build_dir, options.operation, options.cdf_project, env_vars.CDF_PROJECT)
        plan = self._display_setup(options.operation, build_dir, client.config.project, client.console, options.verbose)

        clean_result: Sequence[DeploymentResult] | None = None
        if options.drop and (options.operation == "clean" or not options.dry_run):
            # If we are deploying, and it is dry-run, we skip this step, as apply_plan accounts
            # for drop in dry-run mode.
            clean_result = self.apply_plan(client, list(reversed(plan)), options, is_delete=True)
            if options.operation == "clean":
                return clean_result

        results = self.apply_plan(client, plan, options)

        if clean_result is not None:
            self._merge_clean_results(results, clean_result)

        self._display_results(results, options.operation, client.console, options.verbose)
        self._track_deployment_result(self.tracker, client, results, options.operation)

        if build_lineage and (raw_files := self._find_raw_tables(build_lineage)):
            self._display_deprecation_warning(raw_files, client.console)
            UploadCommand.upload_data(
                raw_files,  # type: ignore[arg-type]
                user_build_dir,
                client,
                options.dry_run,
                client.console,
                options.verbose,
                self.tracker,
            )

        return results

    @classmethod
    def read_build_lineage(cls, build_dir: Path) -> BuildLineage | None:
        if (lineage_path := (build_dir / BuildLineage.filename)).exists():
            return BuildLineage.from_yaml_file(lineage_path)
        return None

    @classmethod
    def read_build_directory(
        cls, build_dir: Path, include: Sequence[str] | None = None, build_lineage: BuildLineage | None = None
    ) -> ReadBuildDirectory:
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
        available_resource_types = set(RESOURCE_CRUD_BY_FOLDER_NAME.keys())
        if include and (invalid := set(include) - available_resource_types):
            raise ToolkitValidationError(
                f"Invalid resource types specified: {humanize_collection(invalid)}, available types: {humanize_collection(available_resource_types)}"
            )
        # Note we support running without linage. This is for example used when deploying resources
        # with the upload command.from
        cdf_project: str | None = None
        if build_lineage:
            build_lineage.validate_source_files_unchanged()
            cdf_project = build_lineage.cdf_project
        include_set = set(include) if include else None
        invalid_resource_dirs: list[Path] = []
        resource_directories: list[ResourceDirectory] = []
        skipped_resource_dirs: list[ResourceDirectory] = []
        for resource_dir in build_dir.iterdir():
            if not resource_dir.is_dir():
                continue
            if resource_dir.name not in RESOURCE_CRUD_BY_FOLDER_NAME:
                invalid_resource_dirs.append(resource_dir)
                continue
            resources = ResourceDirectory(resource_dir)
            cruds = RESOURCE_CRUD_BY_FOLDER_NAME[resource_dir.name]
            for yaml_file in resource_dir.glob("*.yaml"):
                matched = False
                stem = yaml_file.stem.casefold()
                for crud in cruds:
                    if stem.endswith(crud.kind.casefold()):
                        resources.files_by_crud[crud].append(yaml_file)
                        matched = True
                    elif any(stem.endswith(extra_kind.casefold()) for extra_kind in crud.extra_kinds):
                        resources.extra_files.append(yaml_file)
                        matched = True
                if not matched:
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

    def _display_setup(
        self,
        operation: str,
        build_dir: ReadBuildDirectory,
        cdf_project: str,
        console: Console,
        verbose: bool,
    ) -> list[DeploymentStep]:
        startup_section = ToolkitPanelSection(
            content=[
                f"Target project: {cdf_project!r}",
                f"Toolkit version: {__version__!s}",
                f"Build path: {build_dir.path.as_posix()}/",
            ],
        )

        read_dir_warnings = list(build_dir.create_warnings())
        resource_dir_count = len(build_dir.resource_directories)
        skipped_dir_count = len(build_dir.skipped_directories)
        invalid_dir_count = len(build_dir.invalid_directories)
        resource_file_count = sum(
            len(files) for dir_ in build_dir.resource_directories for files in dir_.files_by_crud.values()
        )
        invalid_yaml_file_count = sum(len(dir_.invalid_files) for dir_ in build_dir.resource_directories)
        has_issues = bool(read_dir_warnings or invalid_dir_count or invalid_yaml_file_count)

        read_dir_summary = [
            f"[green]✓[/] [bold]{resource_dir_count}[/] resource directories",
            f"[green]✓[/] [bold]{resource_file_count:,}[/] resource files",
        ]
        if read_dir_warnings:
            read_dir_summary.append(f"[yellow]![/] [bold]{len(read_dir_warnings)}[/] warnings during reading")
        if skipped_dir_count:
            read_dir_summary.append(f"[dim]○[/] [bold]{skipped_dir_count}[/] skipped directories")
        if invalid_dir_count:
            read_dir_summary.append(f"[red]✗[/] [bold]{invalid_dir_count}[/] invalid directories")
        if invalid_yaml_file_count:
            read_dir_summary.append(f"[red]✗[/] [bold]{invalid_yaml_file_count}[/] invalid yaml files")

        read_dir_subsections: list[RenderableType] = [ToolkitPanelSection(content=read_dir_summary)]
        if verbose:
            if build_dir.skipped_directories:
                read_dir_subsections.append(
                    ToolkitPanelSection(
                        title="Skipped directories (excluded by --include)",
                        content=[
                            hanging_indent("○", dir_.directory.as_posix(), marker_style="dim")
                            for dir_ in build_dir.skipped_directories
                        ],
                    )
                )
            if build_dir.invalid_directories:
                read_dir_subsections.append(
                    ToolkitPanelSection(
                        title="Invalid directories (will be skipped)",
                        content=[
                            hanging_indent("✗", inv_dir.as_posix(), marker_style=AuraColor.RED.rich)
                            for inv_dir in build_dir.invalid_directories
                        ],
                    )
                )
            if invalid_yaml_file_count:
                read_dir_subsections.append(
                    ToolkitPanelSection(
                        title="Invalid YAML files (will be skipped)",
                        content=[
                            hanging_indent("✗", file.as_posix(), marker_style=AuraColor.RED.rich)
                            for dir_ in build_dir.resource_directories
                            for file in dir_.invalid_files
                        ],
                    )
                )
        elif skipped_dir_count or invalid_dir_count or invalid_yaml_file_count:
            read_dir_subsections.append(
                ToolkitPanelSection(
                    description=f"{HINT_LEAD_TEXT} Use --verbose to see details about skipped and invalid directories and files."
                )
            )
        read_dir_section = ToolkitPanelSection(
            title="Processed build directory",
            content=read_dir_subsections,
        )

        try:
            plan = self.create_deployment_plan(build_dir)
        except Exception as e:
            console.print(
                ToolkitPanel(
                    Group(
                        Padding(startup_section, (0, 0, 1, 0)),
                        read_dir_section,
                        ToolkitPanelSection(
                            description=f"[bold]Failed to create plan for {operation}:[/] {escape(str(e))}"
                        ),
                    ),
                    title=operation,
                    border_style=AuraColor.AMBER.rich,
                )
            )
            raise

        if not plan:
            plan_section = ToolkitPanelSection(description=f"[bold]No resources to {operation}.[/]")
        else:
            step_count = len(plan)
            total_files = sum(len(step.files) for step in plan)
            plan_section = ToolkitPanelSection(
                title="Plan",
                content=[
                    f"[green]✓[/] [bold]{step_count}[/] resource types to {operation}",
                    f"[green]✓[/] [bold]{total_files}[/] resource files to {operation}",
                ],
            )

        border_style = AuraColor.AMBER.rich if (has_issues or not plan) else AuraColor.GREEN.rich
        console.print(
            ToolkitPanel(
                Group(Padding(startup_section, (0, 0, 1, 0)), read_dir_section, plan_section),
                title=f"Setting up {operation} operation",
                border_style=border_style,
            )
        )
        return plan

    def _validate_cdf_project(
        self,
        build_dir: ReadBuildDirectory,
        operation: str,
        cli_cdf_project: str | None,
        client_cdf_project: str,
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
                f"Enter the name of CDF project you are {operation}ing. This must match the CDF_PROJECT={client_cdf_project!r} in you environment variables.\n",
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
        dependencies_by_crud: dict[type[ResourceIO], Set[type[ResourceIO]]] = {}
        skipped_by_crud: dict[type[ResourceIO], Set[type[ResourceIO]]] = {}
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
    def apply_plan(
        cls, client: ToolkitClient, plan: list[DeploymentStep], options: DeployOptions, is_delete: bool = False
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
            task_id = progress.add_task(f"Starting {options.operation}", total=total_files)
            for step in plan:
                crud = step.crud_cls.create_loader(client)
                resource_name = crud.display_name
                progress.update(task_id, description=f"Reading {resource_name}")

                resource_by_id = cls._read_resource_files(crud, step.files, options)
                if not resource_by_id:
                    # If the CRUD is a GroupScoped and the resources are all scoped.
                    progress.update(task_id, advance=len(step.files))
                    continue
                resource_count = len(resource_by_id)
                request_resources = [resource.request for resource in resource_by_id.values()]

                is_missing_write = cls._validate_access(crud, request_resources, is_dry_run=options.dry_run)

                progress.update(task_id, description=f"Comparing {resource_count} {resource_name} to CDF")
                try:
                    cdf_resource_by_id = {
                        crud.get_id(resource): resource for resource in crud.retrieve(list(resource_by_id.keys()))
                    }
                except ValidationError as validation_error:
                    cls._handle_validation_error(
                        validation_error,
                        "retrieve",
                        crud,
                        [read.request for read in resource_by_id.values()],
                        options.deployment_dir,
                    )
                resources_to_deploy = cls._categorize_resources(
                    crud,
                    resource_by_id,
                    cdf_resource_by_id,
                    console,
                    options,
                    is_delete,
                    is_data_resource=isinstance(crud, ResourceContainerIO),
                )

                if options.dry_run:
                    result = cls.deploy_dry_run(crud, resources_to_deploy, is_missing_write, options)
                    progress.update(task_id, description=f"Would have {options.operation}ed {resource_name} to CDF")
                else:
                    progress.update(task_id, description=f"{options.operation.title()}ing {resource_name} to CDF")
                    result = cls.deploy_resources(crud, resources_to_deploy, step.skipped_cruds, options.deployment_dir)
                    progress.update(task_id, description=f"{options.operation.title()}ed {resource_name} successfully.")

                results.append(result)

                progress.update(task_id, advance=len(step.files))
            finished = "Finished dry-run." if options.dry_run else f"Finished {options.operation}ing."
            progress.update(task_id, description=finished)
        return results

    @classmethod
    def _read_resource_files(
        cls,
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
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
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
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
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
        resource_by_id: dict[T_Identifier, ReadResource[T_RequestResource]],
        cdf_by_id: dict[T_Identifier, T_ResponseResource],
        console: Console,
        options: DeployOptions,
        is_delete: bool,
        is_data_resource: bool,
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

            if is_delete:
                if cdf_resource is None:
                    resources.skipped.append(
                        Skipped(
                            identifier,
                            code="NOT-EXISTING",
                            source_file=resource.source_files[0],
                            reason=f"Will not delete {identifier!s} does not exist in CDF",
                        )
                    )
                    continue
                if not options.drop_data and is_data_resource:
                    resources.skipped.append(
                        Skipped(
                            identifier,
                            code="HAS-DATA",
                            source_file=resource.source_files[0],
                            reason=f"{identifier!s} has data and --drop-data flag is not set, skipping deletion to avoid data loss",
                        )
                    )
                    continue
                resources.to_delete.append(identifier)
            else:
                if cdf_resource is None:
                    resources.to_create.append(resource.request)
                    continue
                cdf_dict = crud.dump_resource(cdf_resource, resource.raw_dict)
                if not options.force_update and cdf_dict == resource.raw_dict:
                    resources.unchanged.append(identifier)
                    continue
                if crud.support_update:
                    resources.to_update.append(resource.request)
                elif is_data_resource:
                    resources.skipped.append(
                        Skipped(
                            identifier,
                            code="HAS-DATA",
                            source_file=resource.source_files[0],
                            reason=(f"{identifier!s} contains data and does not support updates. "),
                        )
                    )
                else:
                    resources.to_delete.append(identifier)
                    resources.to_create.append(resource.request)
                if options.verbose:
                    diff_str = "\n".join(to_diff(cdf_dict, resource.raw_dict))
                    for sensitive in crud.sensitive_strings(resource.request):
                        diff_str = diff_str.replace(sensitive, "********")
                    console.print(
                        ToolkitPanel(
                            diff_str,
                            title=f"{crud.display_name}: {identifier}",
                        )
                    )
        return resources

    @classmethod
    def deploy_dry_run(
        cls,
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        is_missing_write_acl: bool,
        options: DeployOptions,
    ) -> DeploymentResult:
        created = len(resources.to_create)
        updated = len(resources.to_update)
        deleted = len(resources.to_delete)
        unchanged = len(resources.unchanged)

        is_container = isinstance(crud, ResourceContainerIO)
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
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        skipped_cruds: Set[type[ResourceIO]],
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
        except ValidationError as error:
            cls._handle_validation_error(error, action, crud, resources.to_create + resources.to_update, deploy_dir)

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
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: ResourceToDeploy[T_Identifier, T_RequestResource],
        skipped_cruds: Set[type[ResourceIO]],
        deploy_dir: Path | None = None,
    ) -> None:
        if action is None:
            raise RuntimeError("Bug in Toolkit. No action to perform but got API error.") from error
        if error_message := cls._missing_environment_variables(crud, resources, action):
            raise cls._get_resource_exception(action)(error_message) from error

        suffix = ""
        if deploy_dir:
            deploy_dir.mkdir(parents=True, exist_ok=True)
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
    def _handle_validation_error(
        cls,
        error: ValidationError,
        action: Literal["retrieve", "create", "delete", "update"] | None,
        crud: ResourceIO[T_Identifier, T_RequestResource, T_ResponseResource],
        resources: Sequence[T_RequestResource],
        deploy_dir: Path | None = None,
    ) -> None:
        if action is None:
            raise RuntimeError("Bug in Toolkit. No action to perform but got Validation error.") from error

        suffix = ""
        if deploy_dir:
            deploy_dir.mkdir(parents=True, exist_ok=True)
            filepath = deploy_dir / f"{sanitize_filename(datetime.now(timezone.utc).isoformat())}.json"
            suffix = f"\nThe error details has been written to {filepath.as_posix()} for debugging purposes."
            json_str = json.dumps(list(error.errors()), indent=2, sort_keys=False)
            for resource in resources:
                for string in crud.sensitive_strings(resource):
                    json_str = json_str.replace(string, "********")
            filepath.write_text(json_str, encoding="utf-8")

        error_message = f"Failed to {action} {crud.display_name} due to unexpected CDF API response: {error}.{suffix}"
        raise cls._get_resource_exception(action)(error_message) from error

    @classmethod
    def _missing_environment_variables(
        cls, crud: ResourceIO, resources: ResourceToDeploy, action: Literal["create", "delete", "update"]
    ) -> str | None:
        item_ids = resources.get_ids(crud, action)
        match = set(item_ids) & set(resources.missing_env_vars_by_id)
        if not match:
            return None
        missing_variables = [variable for id in match for variable in resources.missing_env_vars_by_id[id]]
        if not missing_variables:
            return None
        variables_str = escape(humanize_collection(missing_variables))
        suffix = "s" if len(missing_variables) > 1 else ""
        return f"\n  {HINT_LEAD_TEXT}This is likely due to missing environment variable{suffix}: {variables_str}"

    @classmethod
    def _get_resource_exception(cls, action: Literal["create", "retrieve", "update", "delete"]) -> type[ToolkitError]:
        return {
            "update": ResourceUpdateError,
            "delete": ResourceDeleteError,
            "create": ResourceCreationError,
            "retrieve": ResourceRetrievalError,
        }[action]

    def _merge_clean_results(
        self, results: Sequence[DeploymentResult], clean_results: Sequence[DeploymentResult]
    ) -> None:
        """Merge results from the clean operation into the deploy results.

        This modifies `results` in place, adding counts from `other` (typically the clean/delete operation).
        Results are matched by resource_name.
        """
        other_by_name = {result.resource_name: result for result in clean_results}
        for result in results:
            if other_result := other_by_name.get(result.resource_name):
                result += other_result

    @classmethod
    def _display_results(
        cls, results: Sequence[DeploymentResult], operation: str, console: Console, verbose: bool
    ) -> None:
        if not results:
            console.print(
                ToolkitPanel(
                    f"No resources were {operation}ed.",
                    title=f"{operation.title()} summary",
                )
            )
            return

        is_dry_run = results[0].is_dry_run
        panel_title = f"{operation.title()} summary"
        if is_dry_run:
            panel_title += " [dim](dry run)[/]"

        table = ToolkitTable()
        table.add_column("Resource", style="cyan")
        if is_dry_run:
            table.add_column("Would create", justify="right", style="green")
            table.add_column("Would update", justify="right", style="yellow")
            table.add_column("Would delete", justify="right", style="red")
        else:
            table.add_column("Created", justify="right", style="green")
            table.add_column("Updated", justify="right", style="yellow")
            table.add_column("Deleted", justify="right", style="red")
        table.add_column("Unchanged", justify="right", style="dim")
        table.add_column("Skipped", justify="right", style="yellow")
        table.add_column("Total", justify="right", style="cyan")
        if is_dry_run:
            table.add_column("Able to deploy", justify="right")

        total = DeploymentResult(
            "All",
            is_dry_run=is_dry_run,
            created_count=0,
            deleted_count=0,
            updated_count=0,
            unchanged_count=0,
            skipped=[],
            is_missing_write_acl=False,
        )
        for result in results:
            row = [
                result.resource_name,
                str(result.created_count),
                str(result.updated_count),
                str(result.deleted_count),
                str(result.unchanged_count),
                str(result.skipped_count),
                str(result.total_count),
            ]
            if is_dry_run:
                row.append("[red]No[/]" if result.is_missing_write_acl else "[green]Yes[/]")
            table.add_row(*row)
            total += result

        if len(results) > 1:
            table.add_section()
            last_row = [
                f"[bold]{total.resource_name}[/]",
                f"[bold]{total.created_count}[/]",
                f"[bold]{total.updated_count}[/]",
                f"[bold]{total.deleted_count}[/]",
                f"[bold]{total.unchanged_count}[/]",
                f"[bold]{total.skipped_count}[/]",
                f"[bold]{total.total_count}[/]",
            ]
            if is_dry_run:
                last_row.append("[red]No[/]" if total.is_missing_write_acl else "[green]Yes[/]")
            table.add_row(*last_row)

        sections: list[RenderableType] = [ToolkitPanelSection(content=[table.as_panel_detail()])]

        if total.skipped and not verbose:
            most_common = Counter(skip.code for skip in total.skipped).most_common(n=3)
            sections.append(
                ToolkitPanelSection(
                    description=(
                        f"{HINT_LEAD_TEXT}A total of {total.skipped_count} resources were skipped during {operation}. "
                        f"The most common reasons were: {', '.join(f'{code} ({count} occurrences)' for code, count in most_common)}. "
                        f"Use --verbose to see all skipped resources."
                    )
                )
            )
        elif verbose and total.skipped:
            sections.append(
                ToolkitPanelSection(
                    title="Skipped resources",
                    content=[
                        hanging_indent(
                            "○",
                            f"[bold]{skip.id}[/] {skip.source_file.as_posix()} [{skip.code}] {skip.reason}",
                            marker_style="dim",
                        )
                        for skip in total.skipped
                    ],
                )
            )

        console.print(ToolkitPanel(Group(*sections), title=panel_title))

    @classmethod
    def _track_deployment_result(
        cls, tracker: Tracker, client: ToolkitClient, results: Sequence[DeploymentResult], operation: str
    ) -> None:
        if not results:
            return

        is_dry_run = results[0].is_dry_run

        # Build flattened per-resource stats for Mixpanel compatibility
        # Creates keys like "dataSets_created", "spaces_updated", etc.
        per_resource_stats: dict[str, int] = {}
        resource_types: list[str] = []
        for result in results:
            # Convert resource name to camelCase key prefix (e.g., "Data Sets" -> "dataSets")
            key_prefix = cls._to_tracking_key(result.resource_name)
            resource_types.append(result.resource_name)
            per_resource_stats[f"{key_prefix}Created"] = result.created_count
            per_resource_stats[f"{key_prefix}Updated"] = result.updated_count
            per_resource_stats[f"{key_prefix}Deleted"] = result.deleted_count
            per_resource_stats[f"{key_prefix}Unchanged"] = result.unchanged_count
            per_resource_stats[f"{key_prefix}Skipped"] = result.skipped_count
            per_resource_stats[f"{key_prefix}Total"] = result.total_count

        event = DeploymentTracking(
            is_dry_run=is_dry_run,
            operation=operation,
            resource_types=resource_types,
            total_created=sum(r.created_count for r in results),
            total_updated=sum(r.updated_count for r in results),
            total_deleted=sum(r.deleted_count for r in results),
            total_unchanged=sum(r.unchanged_count for r in results),
            total_skipped=sum(r.skipped_count for r in results),
            total_resources=sum(r.total_count for r in results),
            resource_type_count=len(results),
            # This pydantic class allows extra
            **per_resource_stats,  # type: ignore [arg-type]
        )
        tracker.track(event, client)

    @staticmethod
    def _to_tracking_key(resource_name: str) -> str:
        """Convert a resource display name to a camelCase tracking key.

        Examples:
            "Data Sets" -> "dataSets"
            "Extraction Pipelines" -> "extractionPipelines"
            "RAW Tables" -> "rawTables"
        """
        words = resource_name.replace("-", " ").split()
        if not words:
            return resource_name.lower()
        return words[0].lower() + "".join(word.capitalize() for word in words[1:])

    @classmethod
    def _find_raw_tables(cls, build_lineage: BuildLineage) -> Mapping[RawTableSelector, list[Path]]:
        selections: dict[RawTableSelector, list[Path]] = defaultdict(list)
        for module in build_lineage.module_lineage:
            for resource in module.resource_lineage:
                if (
                    resource.type.resource_folder == RawTableCRUD.folder_name
                    and resource.type.kind == RawTableCRUD.kind
                    and isinstance(resource.identifier, RawTableId)
                ):
                    for file_type in ["csv", "parquet"]:
                        if (data_file := resource.source_file.with_suffix(f".{file_type}")).is_file():
                            selections[
                                RawTableSelector(
                                    table=SelectedTable(
                                        db_name=resource.identifier.db_name, table_name=resource.identifier.name
                                    )
                                )
                            ].append(data_file)
        return selections

    @classmethod
    def _display_deprecation_warning(cls, raw_files: Mapping[RawTableSelector, list[Path]], console: Console) -> None:
        raw_table_count = len(raw_files)
        file_count = sum(len(files) for files in raw_files.values())
        plural_t = "" if raw_table_count == 1 else "s"
        plural_f = "" if file_count == 1 else "s"
        console.print(
            ToolkitPanel(
                Group(
                    ToolkitPanelSection(
                        description=f"[bold]{raw_table_count}[/] raw table{plural_t} from [bold]{file_count}[/] file{plural_f}."
                    ),
                    ToolkitPanelSection(
                        description=(
                            "Support for deploying raw tables through the deploy command will be removed in a future release. "
                            "Please migrate your raw tables to use the new data plugin. See the documentation for more details."
                        )
                    ),
                ),
                title="Deprecation warning",
                border_style=AuraColor.AMBER.rich,
            )
        )
