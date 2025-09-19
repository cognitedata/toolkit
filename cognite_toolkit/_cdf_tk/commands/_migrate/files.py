from pathlib import Path

import questionary
from cognite.client.data_classes.capabilities import (
    Capability,
    DataSetScope,
    FilesAcl,
)
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from rich.progress import track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_filemetdata import ExtendedFileMetadata
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .base import BaseMigrateCommand
from .data_classes import MigrationMappingList


class MigrateFilesCommand(BaseMigrateCommand):
    cdf_cdm = "cdf_cdm"
    view_id = ViewId(cdf_cdm, "CogniteFile", "v1")
    chunk_size = 1000

    def source_acl(self, data_set_ids: list[int]) -> Capability:
        return FilesAcl(
            actions=[FilesAcl.Action.Read, FilesAcl.Action.Write],
            scope=DataSetScope(data_set_ids),
        )

    def migrate_files(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        verbose: bool = False,
        auto_yes: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = MigrationMappingList.read_mapping_file(mapping_file, "file")
        self.validate_access(
            client,
            instance_spaces=list(mappings.spaces()),
            schema_spaces=[self.cdf_cdm],
            data_set_ids=list(mappings.get_data_set_ids()),
        )
        self._validate_files(client, mappings)
        self.validate_available_capacity(client, len(mappings))

        if dry_run:
            self.console(f"Dry run mode. Would have migrated {len(mappings):,} Files to CogniteFiles.")
            return
        if not auto_yes and self._confirm(mappings) is False:
            return
        self._migrate(client, mappings, verbose)

    def _validate_files(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        total_validated = 0
        chunk: MigrationMappingList
        for chunk in track(
            chunker_sequence(mappings, size=self.chunk_size),
            description="Validating...",
            total=len(mappings) // self.chunk_size + 1,
        ):
            try:
                files = client.files.retrieve_multiple(
                    ids=chunk.get_ids(),
                    ignore_unknown_ids=True,
                )
            except CogniteAPIError as e:
                raise AuthenticationError(
                    f"Failed to retrieve Files. This is likely due to lack of permissions: {e!s}"
                ) from e

            missing_count = len(files) - len(mappings)
            if missing_count > 0:
                raise ToolkitValueError(f"{missing_count} Files are missing in CDF.")

            missing_file_content = [file for file in files if file.uploaded is not True]
            if missing_file_content:
                raise ToolkitValueError(
                    f"The following files does not have file content yet: {humanize_collection(missing_file_content)}. "
                    "You can only migrate files that have file content uploaded."
                )

            existing_result = client.data_modeling.instances.retrieve(chunk.as_node_ids())
            if len(existing_result.nodes) != 0:
                raise ToolkitValueError(
                    "Some of the Files you are trying to migrate already exist in Data Modeling. "
                    f"Please remove the following files from the mapping file {humanize_collection(existing_result.nodes.as_ids())}"
                )
            total_validated += len(files)
        print(
            f"Validated {total_validated:,} Files for migration. "
            f"{len(mappings):,} mappings provided in the mapping file."
        )

    @staticmethod
    def _confirm(mappings: MigrationMappingList) -> bool:
        print(
            Panel(
                f"[red]WARNING:[/red] This operation [bold]cannot be undone[/bold]! "
                f"{len(mappings):,} Files will linked to the new CogniteFiles. "
                "This linking cannot be undone",
                style="bold",
                title="Migrate asset-centric Files to CogniteFiles",
                title_align="left",
                border_style="red",
                expand=False,
            )
        )

        if not questionary.confirm("Are you really sure you want to continue?", default=False).ask():
            print("Migration cancelled by user.")
            return False
        return True

    def _migrate(self, client: ToolkitClient, mappings: MigrationMappingList, verbose: bool) -> None:
        print("Migrating Files to CogniteFiles...")
        total_migrated = 0
        for chunk in track(
            chunker_sequence(mappings, size=self.chunk_size),
            description="Migrating Files to CogniteFiles...",
            total=len(mappings) // self.chunk_size + 1,
        ):
            if verbose:
                print(f"Migrating {len(chunk):,} Files...")

            # Set pending IDs for the chunk of mappings
            try:
                pending_files = client.files.set_pending_ids(chunk.as_pending_ids())
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to set pending IDs for Files: {e!s}") from e

            # The ExtendedTimeSeriesList is iterating ExtendedTimeSeries objects.
            converted_files = [self.as_cognite_file(file) for file in pending_files]  # type: ignore[arg-type]
            try:
                created = client.data_modeling.instances.apply_fast(converted_files)
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to apply Files: {e!s}") from e
            if verbose:
                print(f"Created {len(created):,} CogniteFiles.")
            total_migrated += len(created)
        print(f"Successfully migrated {total_migrated:,} Files to CogniteFiles.")

    @classmethod
    def as_cognite_file(cls, file: ExtendedFileMetadata) -> CogniteFileApply:
        if file.pending_instance_id is None:
            raise ToolkitValueError("ExtendedFiles must have a pending_instance_id set before migration.")

        return CogniteFileApply(
            space=file.pending_instance_id.space,
            external_id=file.pending_instance_id.external_id,
            name=file.name,
            mime_type=file.mime_type,
        )
