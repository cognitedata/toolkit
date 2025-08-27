from uuid import uuid4

from cognite.client.exceptions import CogniteException

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    Canvas,
    ContainerReferenceApply,
    FdmInstanceContainerReferenceApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.interactive_select import InteractiveCanvasSelect

from .base import BaseMigrateCommand
from .data_model import INSTANCE_SOURCE_VIEW_ID


class MigrationCanvasCommand(BaseMigrateCommand):
    canvas_schema_space = Canvas.get_source().space
    # Note sequences are not supported in Canvas, so we do not include them here.
    asset_centric_resource_types = frozenset({"asset", "event", "file", "timeseries"})

    def migrate_canvas(
        self,
        client: ToolkitClient,
        external_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self.validate_access(
            client,
            instance_spaces=[CANVAS_INSTANCE_SPACE],
            schema_spaces=[self.canvas_schema_space, INSTANCE_SOURCE_VIEW_ID.space],
        )
        self.validate_migration_model_available(client)
        external_ids = external_ids or InteractiveCanvasSelect(client).select_external_ids()
        if external_ids is None or not external_ids:
            self.console("No canvases selected for migration.")
            return
        action = "Would migrate" if dry_run else "Migrating"
        self.console(f"{action} {len(external_ids)} canvases.")
        for external_id in external_ids:
            self._migrate_single_canvas(client, external_id, dry_run=dry_run, verbose=verbose)

    def _migrate_single_canvas(
        self,
        client: ToolkitClient,
        external_id: str,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        canvas = client.canvas.industrial.retrieve(external_id=external_id)
        if canvas is None:
            self.warn(MediumSeverityWarning(f"Canvas with external ID '{external_id}' not found. Skipping.. "))
            return
        update = canvas.as_write()
        to_migrate = [
            ref
            for ref in update.container_references
            if ref.container_reference_type in self.asset_centric_resource_types
        ]
        if not to_migrate:
            self.warn(
                LowSeverityWarning(
                    f"Canvas with name '{canvas.canvas.name}' does not have any asset-centric references. Skipping.. "
                )
            )
        if verbose:
            self.console(f"Found canvas: {canvas.canvas.name}")
        reference_ids = [ref.as_asset_centric_id() for ref in to_migrate]
        instance_sources = client.migration.instance_source.retrieve(reference_ids)
        source_by_reference_id = {source.as_asset_centric_id(): source for source in instance_sources}
        missing = set(reference_ids) - set(source_by_reference_id.keys())
        if missing:
            self.warn(
                HighSeverityWarning(
                    f"Canvas '{canvas.canvas.name}' has references to resources that are not been migrated: {humanize_collection(missing)}. Skipping.. "
                )
            )
            return
        if dry_run:
            self.console(
                f"Canvas '{canvas.canvas.name}' is ready for migration all {len(instance_sources)} references asset-centric resources found."
            )
            return
        if verbose:
            self.console(
                f"Migrating canvas '{canvas.canvas.name}' with {len(instance_sources)} references to asset-centric resources."
            )
        backup = canvas.as_write().create_backup()

        update.container_references = [
            ref
            for ref in update.container_references
            if ref.container_reference_type not in self.asset_centric_resource_types
        ]
        for ref in to_migrate:
            source = source_by_reference_id[ref.as_asset_centric_id()]
            fdm_ref = self.migrate_container_reference(ref, source, canvas.canvas.external_id)
            update.fdm_instance_container_references.append(fdm_ref)

        try:
            client.canvas.industrial.create(backup)
        except CogniteException as e:
            raise ToolkitMigrationError(f"Failed to create backup for canvas '{canvas.canvas.name}': {e!s}. ") from e
        try:
            client.canvas.industrial.update(update)
        except CogniteException as e:
            raise ToolkitMigrationError(
                f"Failed to migrate canvas '{canvas.canvas.name}': {e!s}. A backup was created with external ID '{backup.canvas.external_id}'."
            ) from e
        else:
            self.console(
                f'Canvas "{canvas.canvas.name}" migrated successfully with {len(to_migrate)} references to data model instances.'
            )

    @classmethod
    def migrate_container_reference(
        cls, reference: ContainerReferenceApply, source: InstanceSource, canvas_external_id: str
    ) -> FdmInstanceContainerReferenceApply:
        """Migrate a single container reference by replacing the asset-centric ID with the data model instance ID."""
        consumer_view = source.consumer_view()
        new_external_id = f"{canvas_external_id}_{uuid4()}"
        return FdmInstanceContainerReferenceApply(
            external_id=new_external_id,
            id_=reference.id_,
            container_reference_type="fdmInstance",
            instance_space=source.space,
            instance_external_id=source.external_id,
            view_space=consumer_view.space,
            view_external_id=consumer_view.external_id,
            view_version=consumer_view.version,
            label=reference.label,
            properties_=reference.properties_,
            x=reference.x,
            y=reference.y,
            width=reference.width,
            height=reference.height,
            max_width=reference.max_width,
            max_height=reference.max_height,
        )
