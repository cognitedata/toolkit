from pathlib import Path

import questionary
from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    DataSetScope,
    SpaceIDScope,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import DirectRelationReference, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from rich.progress import track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence

from .data_classes import MigrationMappingList


class MigrateTimeseriesCommand(ToolkitCommand):
    cdf_cdm = "cdf_cdm"
    view_id = ViewId(cdf_cdm, "CogniteTimeSeries", "v1")
    cdf_cdm_units = "cdf_cdm_units"
    schema_spaces = frozenset({cdf_cdm, cdf_cdm_units})
    chunk_size = 1000

    def migrate_timeseries(
        self,
        client: ToolkitClient,
        mapping_file: Path,
        dry_run: bool = False,
        verbose: bool = False,
        auto_yes: bool = False,
    ) -> None:
        """Migrate resources from Asset-Centric to data modeling in CDF."""
        mappings = MigrationMappingList.read_mapping_file(mapping_file)

        self._validate_access(client, mappings)
        self._validate_timeseries_existence(client, mappings)
        self._validate_available_capacity(client, mappings)

        if dry_run:
            self.console(f"Dry run mode. Would have migrated {len(mappings):,} TimeSeries to CogniteTimeSeries.")
            return
        if not auto_yes and self._confirm(mappings) is False:
            return
        self._migrate(client, mappings, verbose)

    def _validate_access(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        required_capabilities: list[Capability] = [
            DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=SpaceIDScope(list(self.schema_spaces))),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                    DataModelInstancesAcl.Action.Write,
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=SpaceIDScope(list(mappings.spaces())),
            ),
        ]
        if data_set_ids := mappings.get_data_set_ids():
            required_capabilities.append(
                TimeSeriesAcl(
                    actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
                    scope=DataSetScope(list(data_set_ids)),
                )
            )
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.")

    def _validate_timeseries_existence(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        total_validated = 0
        chunk: MigrationMappingList
        for chunk in track(  # type: ignore[assignment]
            chunker_sequence(mappings, size=self.chunk_size),
            description="Validating...",
            total=len(mappings) // self.chunk_size + 1,
        ):
            try:
                timeseries = client.time_series.retrieve_multiple(
                    ids=chunk.get_ids(),
                    external_ids=chunk.get_external_ids(),
                    ignore_unknown_ids=True,
                )
            except CogniteAPIError as e:
                raise AuthenticationError(
                    f"Failed to retrieve TimeSeries. This is likely due to lack of permissions: {e!s}"
                ) from e

            missing_count = len(timeseries) - len(mappings)
            if missing_count > 0:
                raise ToolkitValueError(f"Missing {missing_count} TimeSeries does not exist in CDF.")

            existing_result = client.data_modeling.instances.retrieve(chunk.as_node_ids())
            if len(existing_result.nodes) != 0:
                raise ToolkitValueError(
                    "Some of the TimeSeries you are trying to migrate already exist in Data Modeling. "
                    f"Please remove the following TimeSeries from the mapping file {humanize_collection(existing_result.nodes.as_ids())}"
                )
            total_validated += len(timeseries)
        print(
            f"Validated {total_validated:,} TimeSeries for migration. "
            f"{len(mappings):,} mappings provided in the mapping file."
        )

    def _validate_available_capacity(self, client: ToolkitClient, mappings: MigrationMappingList) -> None:
        """Validate that the project has enough capacity to accommodate the migration."""
        try:
            stats = client.data_modeling.statistics.project()
        except CogniteAPIError:
            # This endpoint is not yet in alpha, it may change or not be available.
            self.warn(HighSeverityWarning("Cannot check the instances capacity proceeding with migration anyway."))
            return
        available_capacity = stats.instances.instances_limit - stats.instances.instances
        available_capacity_after = available_capacity - len(mappings)

        if available_capacity_after < DMS_INSTANCE_LIMIT_MARGIN:
            raise ToolkitValueError(
                "Cannot proceed with migration, not enough instance capacity available. Total capacity after migration"
                f"would be {available_capacity_after:,} instances, which is less than the required margin of"
                f"{DMS_INSTANCE_LIMIT_MARGIN:,} instances. Please increase the instance capacity in your CDF project"
                f" or delete some existing instances before proceeding with the migration of {len(mappings):,} timeseries."
            )
        total_instances = stats.instances.instances + len(mappings)
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )

    @staticmethod
    def _confirm(mappings: MigrationMappingList) -> bool:
        print(
            Panel(
                f"[red]WARNING:[/red] This operation [bold]cannot be undone[/bold]! "
                f"{len(mappings):,} TimeSeries will linked to the new CogniteTimeSeries. "
                "This linking cannot be undone",
                style="bold",
                title="Migrate asset-centric TimeSeries to CogniteTimeSeries",
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
        print("Migrating TimeSeries to CogniteTimeSeries...")
        total_migrated = 0
        for chunk in track(
            chunker_sequence(mappings, size=self.chunk_size),
            description="Migrating TimeSeries to CogniteTimeSeries...",
            total=len(mappings) // self.chunk_size + 1,
        ):  # type: ignore[assignment]
            if verbose:
                print(f"Migrating {len(chunk):,} TimeSeries...")

            # Set pending IDs for the chunk of mappings
            try:
                pending_timeseries = client.time_series.set_pending_ids(chunk.as_pending_ids())
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to set pending IDs for TimeSeries: {e!s}") from e

            # The ExtendedTimeSeriesList is iterating ExtendedTimeSeries objects.
            converted_timeseries = [self.as_cognite_timeseries(ts) for ts in pending_timeseries]  # type: ignore[arg-type]
            try:
                created = client.data_modeling.instances.apply_fast(converted_timeseries)
            except CogniteAPIError as e:
                raise ToolkitValueError(f"Failed to apply TimeSeries: {e!s}") from e
            if verbose:
                print(f"Created {len(created):,} CogniteTimeSeries.")
            total_migrated += len(created)
        print(f"Successfully migrated {total_migrated:,} TimeSeries to CogniteTimeSeries.")

    @classmethod
    def as_cognite_timeseries(cls, ts: ExtendedTimeSeries) -> CogniteTimeSeriesApply:
        if ts.pending_instance_id is None:
            raise ToolkitValueError("ExtendedTimeSeries must have a pending_instance_id set before migration.")
        if ts.is_step is None:
            raise ToolkitValueError("ExtendedTimeSeries must have is_step set before migration.")

        return CogniteTimeSeriesApply(
            space=ts.pending_instance_id.space,
            external_id=ts.pending_instance_id.external_id,
            name=ts.name,
            description=ts.description,
            is_step=ts.is_step,
            time_series_type="string" if ts.is_string else "numeric",
            source_unit=ts.unit,
            unit=DirectRelationReference(cls.cdf_cdm_units, ts.unit_external_id) if ts.unit_external_id else None,
        )
