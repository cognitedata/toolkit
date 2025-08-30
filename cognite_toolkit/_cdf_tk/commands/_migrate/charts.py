from cognite.client.exceptions import CogniteException

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.charts import ChartWrite
from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, InstanceSource
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import BaseMigrateCommand


class MigrationChartCommand(BaseMigrateCommand):
    @property
    def schema_spaces(self) -> list[str]:
        return []

    def migrate_charts(
        self,
        client: ToolkitClient,
        external_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self.validate_instance_source_exists(client)
        if external_ids is None:
            raise ToolkitNotImplementedError("Interactive selection of charts is not implemented yet.")
        if external_ids is None or not external_ids:
            self.console("No charts selected for migration.")
            return
        action = "Would migrate" if dry_run else "Migrating"
        self.console(f"{action} {len(external_ids)} charts.")
        for external_id in external_ids:
            self._migrate_single_chart(client, external_id, dry_run=dry_run, verbose=verbose)

    def _migrate_single_chart(
        self,
        client: ToolkitClient,
        external_id: str,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        chart = client.charts.retrieve(external_id=external_id)
        if chart is None:
            self.warn(MediumSeverityWarning(f"Chart with external ID '{external_id}' not found. Skipping.. "))
            return
        ts_to_migrate = [ref for ref in chart.data.time_series_collection or [] if ref.ts_id is not None]
        if not ts_to_migrate:
            self.warn(LowSeverityWarning(f"Chart with external ID '{external_id}' has no time series. Skipping.. "))
            return
        if verbose:
            self.console(f"Chart '{external_id}' references {humanize_collection(ts_to_migrate)} time series.")
        timeseries_ids = [AssetCentricId("timeseries", id_=ref.ts_id) for ref in ts_to_migrate if ref.ts_id is not None]
        instance_sources = client.migration.instance_source.retrieve(timeseries_ids)
        source_by_reference_id = {source.as_asset_centric_id(): source for source in instance_sources}
        missing = set(timeseries_ids) - set(source_by_reference_id.keys())
        if missing:
            self.warn(
                HighSeverityWarning(
                    f"Chart with external ID '{external_id}' references {humanize_collection(missing)} time series "
                    "which have not been migrated. Skipping... "
                )
            )
            return
        if dry_run:
            self.console(
                f"Chart '{chart.data.name or chart.external_id}' is ready for migration all {len(instance_sources)} referenced timeseries found."
            )
            return

        if verbose:
            self.console(
                f"Migrating chart '{chart.data.name or chart.external_id}' with {len(instance_sources)} referenced timeseries."
            )

        update = self.migrate_chart_data(chart.as_write(), source_by_reference_id, verbose=verbose)

        try:
            updated = client.charts.upsert(update)
        except CogniteException as e:
            raise ToolkitMigrationError(f"Failed to upsert chart '{updated.data.name or updated.external_id}'") from e
        else:
            if verbose:
                self.console(f"Chart '{updated.data.name or updated.external_id}' migrated successfully.")

    @classmethod
    def migrate_chart_data(
        cls, chart: ChartWrite, source_by_reference_id: dict[AssetCentricId, InstanceSource], verbose: bool = False
    ) -> ChartWrite:
        raise NotImplementedError()
