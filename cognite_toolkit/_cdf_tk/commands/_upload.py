from collections.abc import Sequence
from functools import partial
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResource
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DirectRelation,
    MappedProperty,
    RequiresConstraint,
    ViewId,
)
from pydantic import ValidationError
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import T_Selector, UploadableStorageIO, are_same_kind, get_upload_io
from cognite_toolkit._cdf_tk.storageio._base import T_WriteCogniteResource, TableUploadableStorageIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import Selector, SelectorAdapter
from cognite_toolkit._cdf_tk.storageio.selectors._instances import InstanceSpaceSelector, InstanceViewSelector
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.utils.fileio import TABLE_READ_CLS_BY_FORMAT, FileReader
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemMessage, SuccessResponseItems
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

from ._base import ToolkitCommand
from .deploy import DeployCommand


class UploadCommand(ToolkitCommand):
    """Command for uploading data to CDF from files in a specified directory.

    This command reads files matching a specific pattern in the input directory,
    processes them using a StorageIO instance, and uploads the data to CDF.

    Attributes:
        _MAX_QUEUE_SIZE (int): The maximum size of the queue for processing items.
            Set to 80 to balance memory usage and processing speed.
    """

    _MAX_QUEUE_SIZE = 80
    _UPLOAD = "upload"

    def upload(
        self,
        input_dir: Path,
        client: ToolkitClient,
        deploy_resources: bool,
        dry_run: bool,
        verbose: bool,
        kind: str | None = None,
    ) -> None:
        """Uploads data from files in the specified input directory to CDF.

        Args:
            input_dir: The directory containing the files to upload. It is expected to
                have the structure defined below.
            client: An instance of ToolkitClient to interact with CDF.
            deploy_resources: If True, deploys resources from the 'resources' subdirectory.
            dry_run: If True, performs a dry run without actually uploading the data
                (or deploying resources).
            verbose: If True, prints detailed information about the upload process.
            kind: Optional; if provided, only data files of this kind will be processed.

        The expected structure of the input directory is as follows:
        ```
        input_dir/
        ├── resources/                # Optional, only if deploy_resources is True
        │   ├── raw/
        │   │   ├── table1.Table.yaml
        │   │   └── table2.Table.yaml
        │   └── ...
        ├── datafile1.kind.ndjson # Data file of a specific kind
        ├── datafile1.Metadata.yaml       # Metadata file for datafile1
        ├── datafile2.kind2.ndjson # Another data file of the same or different kind
        ├── datafile2.Metadata.yaml       # Metadata file for datafile2
        └── ...
        """
        console = Console()
        data_files_by_selector = self._find_data_files(input_dir, kind)

        self._deploy_resource_folder(input_dir / DATA_RESOURCE_DIR, deploy_resources, client, console, dry_run, verbose)

        # Calculate dependency order for data model instances to ensure
        # that referenced entities are created before entities that reference them
        view_dependencies = self._calculate_dependency_order(data_files_by_selector, client)
        if len(view_dependencies) > 0:
            # Build a lookup map from ViewId to Selector for efficient access
            selector_by_view: dict[ViewId, Selector] = {}
            for selector in data_files_by_selector:
                if isinstance(selector, InstanceViewSelector | InstanceSpaceSelector) and selector.view is not None:
                    selector_by_view[selector.view.as_id()] = selector

            # Reorder selectors according to the dependency-sorted view list
            ordered_selectors: dict[Selector, list[Path]] = {}
            for view_id in view_dependencies:
                if view_id in selector_by_view:
                    selector = selector_by_view[view_id]
                    ordered_selectors[selector] = data_files_by_selector[selector]

            # Preserve selectors that aren't affected by view dependencies
            # (e.g., raw tables, time series, non-view instance data)
            for selector in data_files_by_selector.keys():
                if selector not in ordered_selectors:
                    ordered_selectors[selector] = data_files_by_selector[selector]
            data_files_by_selector = ordered_selectors
        self._upload_data(data_files_by_selector, client, dry_run, input_dir, console, verbose)

    def _find_container_dependencies(
        self,
        container_id: ContainerId,
        dependent_containers_path: list[ContainerId],
        container_dependencies: dict[ContainerId, list[ContainerId]],
        client: ToolkitClient,
    ) -> None:
        """Recursively find all container dependencies based on constraints and direct relations.

        This method traverses the container dependency graph to identify which containers
        must be populated before others. Dependencies arise from:
        1. RequiresConstraint: Explicit container requirements.
        2. DirectRelation properties that have a required type.

        Args:
            container_id: The container to analyze for dependencies.
            dependent_containers_path: Chain of containers that depend on this one, used to
                track the dependency path during recursion.
            container_dependencies: Dictionary mapping each container to its list of
                required containers. Updated in-place during traversal.
            client: ToolkitClient instance for retrieving container metadata.
        """
        container = client.data_modeling.containers.retrieve(container_id)
        if container is None:
            self.warn(
                MediumSeverityWarning(
                    f"Container {container_id} not found or you don't have permission to access it, skipping dependency check."
                )
            )
            return
        if container_id not in container_dependencies:
            container_dependencies[container_id] = []
        required_containers: list[ContainerId] = []
        for constraint in container.constraints.values():
            if isinstance(constraint, RequiresConstraint):
                for dependent_container in dependent_containers_path:
                    container_dependencies[dependent_container].append(constraint.require)
                required_containers.append(constraint.require)
        for property in container.properties.values():
            if (
                isinstance(property, MappedProperty)
                and isinstance(property.type, DirectRelation)
                and property.type.container is not None
            ):
                for dependent_container in dependent_containers_path:
                    container_dependencies[dependent_container].append(property.type.container)
                required_containers.append(property.type.container)
        for required_container_id in required_containers:
            if required_container_id in container_dependencies:
                continue
            self._find_container_dependencies(
                required_container_id,
                [*dependent_containers_path, required_container_id],
                container_dependencies,
                client,
            )

    def _calculate_dependency_order(
        self, data_files_by_selector: dict[Selector, list[Path]], client: ToolkitClient
    ) -> list[ViewId]:
        """Calculate the necessary upload order for views based on container constraints.

        This method analyzes the selected views to determine which must be
        populated before others to satisfy container constraints or required types for direct relations.

        The dependency analysis works as follows:
        1. Extract all views referenced in selectors
        2. Map which containers are populated by each view
        3. Find container dependencies (via _find_container_dependencies)
        4. Derive the dependency of views on other views from container dependencies
        5. Topologically sort views to respect dependencies

        Args:
            data_files_by_selector: Mapping of selectors to their data files.
            client: ToolkitClient instance for retrieving view and container metadata.

        Returns:
            list[ViewId]: Ordered list of ViewIds according to dependencies.
        """
        view_to_containers: dict[ViewId, list[ContainerId]] = {}
        container_dependencies: dict[ContainerId, list[ContainerId]] = {}
        container_to_views: dict[ContainerId, set[ViewId]] = {}
        view_dependencies: dict[ViewId, set[ViewId]] = {}

        all_view_ids = [selector.view.as_id() for selector in data_files_by_selector if selector.view is not None]
        if not all_view_ids:
            return []

        views = client.data_modeling.views.retrieve(all_view_ids)
        missing_view_ids = set(all_view_ids) - set(views.as_ids())
        if missing_view_ids:
            self.warn(
                MediumSeverityWarning(
                    f"Views {missing_view_ids} not found or you don't have permission to access them, skipping dependency check."
                )
            )
        for view in views:
            view_to_containers[view.as_id()] = []
            for property in view.properties.values():
                if not isinstance(property, MappedProperty):
                    continue
                if property.container not in container_to_views:
                    container_to_views[property.container] = set()
                container_to_views[property.container].add(view.as_id())
                if property.container not in view_to_containers[view.as_id()]:
                    view_to_containers[view.as_id()].append(property.container)

        for container_id in container_to_views:
            self._find_container_dependencies(container_id, [container_id], container_dependencies, client)

        for view_id, containers in view_to_containers.items():
            required_views: set[ViewId] = set()
            for container_id in containers:
                for required_container in container_dependencies.get(container_id, []):
                    if required_container in containers:
                        continue
                    views_populated_by_container = container_to_views.get(required_container, set())
                    required_views.update(views_populated_by_container)
            view_dependencies[view_id] = required_views

        try:
            sorted_views = list(TopologicalSorter(view_dependencies).static_order())
        except CycleError as e:
            self.warn(
                MediumSeverityWarning(
                    f"Circular dependency detected in views: {e}. Upload order may not respect all dependencies."
                )
            )
            sorted_views = list(view_dependencies.keys())
        return sorted_views

    def _find_data_files(
        self,
        input_dir: Path,
        kind: str | None = None,
    ) -> dict[Selector, list[Path]]:
        """Finds data files and their corresponding metadata files in the input directory."""
        manifest_file_endswith = f".{DATA_MANIFEST_STEM}.yaml"
        data_files_by_metadata: dict[Selector, list[Path]] = {}
        for metadata_file in input_dir.glob(f"*{manifest_file_endswith}"):
            data_file_prefix = metadata_file.name.removesuffix(manifest_file_endswith)
            data_files = [
                file
                for file in input_dir.glob(f"{data_file_prefix}*")
                if not file.name.endswith(manifest_file_endswith)
            ]
            if kind is not None and data_files:
                data_files = [data_file for data_file in data_files if are_same_kind(kind, data_file)]
                if not data_files:
                    continue
            if not data_files:
                self.warn(
                    MediumSeverityWarning(
                        f"Metadata file {metadata_file.as_posix()!r} has no corresponding data files, skipping.",
                    )
                )
                continue

            selector_dict = read_yaml_file(metadata_file, expected_output="dict")
            try:
                selector = SelectorAdapter.validate_python(selector_dict)
            except ValidationError as e:
                errors = humanize_validation_error(e)
                self.warn(
                    ResourceFormatWarning(
                        metadata_file, tuple(errors), text="Invalid selector in metadata file, skipping."
                    )
                )
                continue
            data_files_by_metadata[selector] = data_files
        return data_files_by_metadata

    def _deploy_resource_folder(
        self,
        resource_dir: Path,
        deploy_resources: bool,
        client: ToolkitClient,
        console: Console,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        """Deploy resources from the specified resource directory if it exists and deployment is enabled."""
        if deploy_resources and resource_dir.exists():
            deploy_command = DeployCommand()
            deploy_results = deploy_command.deploy_all_resources(
                client=client,
                build_dir=resource_dir,
                env_vars=EnvironmentVariables.create_from_environment(),
                dry_run=dry_run,
                verbose=verbose,
            )
            self.warning_list.extend(deploy_command.warning_list)
            if deploy_results.has_counts:
                console.print(deploy_results.counts_table())
        elif deploy_resources:
            self.warn(
                MediumSeverityWarning(
                    f"Resource directory {resource_dir.as_posix()!r} does not exist, skipping resource deployment."
                )
            )

    def _upload_data(
        self,
        data_files_by_selector: dict[Selector, list[Path]],
        client: ToolkitClient,
        dry_run: bool,
        input_dir: Path,
        console: Console,
        verbose: bool,
    ) -> None:
        total_file_count = sum(len(files) for files in data_files_by_selector.values())
        if verbose:
            input_dir_display = self._path_as_display_name(input_dir)
            console.print(f"Found {total_file_count} files to upload in {input_dir_display.as_posix()!r}.")
        action = "Would upload" if dry_run else "Uploading"
        with HTTPClient(config=client.config) as upload_client:
            file_count = 1
            for selector, datafiles in data_files_by_selector.items():
                io = self._create_selected_io(selector, datafiles[0], client)
                if io is None:
                    continue
                for data_file in datafiles:
                    file_display = self._path_as_display_name(data_file)
                    if verbose:
                        console.print(f"{action} {selector.display_name} from {file_display.as_posix()!r}")
                    reader = FileReader.from_filepath(data_file)
                    is_table = reader.format in TABLE_READ_CLS_BY_FORMAT
                    if is_table and not isinstance(io, TableUploadableStorageIO):
                        raise ToolkitValueError(f"{selector.display_name} does not support {reader.format!r} files.")
                    tracker = ProgressTracker[str]([self._UPLOAD])
                    data_name = "row" if is_table else "line"
                    executor = ProducerWorkerExecutor[list[tuple[str, dict[str, JsonVal]]], Sequence[UploadItem]](
                        download_iterable=chunker(
                            ((f"{data_name} {line_no}", item) for line_no, item in enumerate(reader.read_chunks(), 1)),
                            io.CHUNK_SIZE,
                        ),
                        process=partial(io.rows_to_data, selector=selector)
                        if is_table and isinstance(io, TableUploadableStorageIO)
                        else io.json_chunk_to_data,
                        write=partial(
                            self._upload_items,
                            upload_client=upload_client,
                            io=io,
                            dry_run=dry_run,
                            selector=selector,
                            tracker=tracker,
                            console=console,
                        ),
                        iteration_count=None,
                        max_queue_size=self._MAX_QUEUE_SIZE,
                        download_description=f"Reading {file_count:,}/{total_file_count + 1:,}: {file_display.as_posix()!s}",
                        process_description="Processing",
                        write_description=f"{action} {selector.display_name!r}",
                        console=console,
                    )
                    executor.run()
                    file_count += 1
                    executor.raise_on_error()
                    final_action = "Uploaded" if not dry_run else "Would upload"
                    suffix = " successfully" if not dry_run else ""
                    results = tracker.aggregate()
                    success = results.get((self._UPLOAD, "success"), 0)
                    failed = results.get((self._UPLOAD, "failed"), 0)
                    if failed > 0:
                        suffix += f", {failed:,} failed"
                    console.print(
                        f"{final_action} {success:,} {selector.display_name} from {file_display.as_posix()!r}{suffix}."
                    )

    @staticmethod
    def _path_as_display_name(input_path: Path, cwd: Path = Path.cwd()) -> Path:
        display_name = input_path
        if input_path.is_relative_to(cwd):
            display_name = input_path.relative_to(cwd)
        return display_name

    def _create_selected_io(
        self, selector: Selector, data_file: Path, client: ToolkitClient
    ) -> UploadableStorageIO | None:
        try:
            io_cls = get_upload_io(type(selector), kind=data_file)
        except ValueError as e:
            self.warn(HighSeverityWarning(f"Could not find StorageIO for selector {selector}: {e}"))
            return None
        return io_cls(client)

    @classmethod
    def _upload_items(
        cls,
        data_chunk: Sequence[UploadItem],
        upload_client: HTTPClient,
        io: UploadableStorageIO[T_Selector, T_CogniteResource, T_WriteCogniteResource],
        selector: T_Selector,
        dry_run: bool,
        tracker: ProgressTracker[str],
        console: Console,
    ) -> None:
        if dry_run:
            for item in data_chunk:
                tracker.set_progress(item.source_id, cls._UPLOAD, "success")
            return
        results = io.upload_items(data_chunk, upload_client, selector)
        for message in results:
            if isinstance(message, SuccessResponseItems):
                for id_ in message.ids:
                    tracker.set_progress(id_, step=cls._UPLOAD, status="success")
            elif isinstance(message, ItemMessage):
                for id_ in message.ids:
                    tracker.set_progress(id_, step=cls._UPLOAD, status="failed")
            else:
                console.log(f"[red]Unexpected result from upload: {str(message)!r}[/red]")
