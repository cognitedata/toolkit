from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl, DataModelsAcl, SpaceIDScope

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    Canvas,
    ContainerReferenceApply,
    FdmInstanceContainerReferenceApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.migration import Mapping
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.interactive_select import InteractiveCanvasSelect

from .data_model import MAPPING_VIEW_ID


class MigrationCanvasCommand(ToolkitCommand):
    canvas_schema_space = Canvas.get_source().space

    def migrate_canvas(
        self,
        client: ToolkitClient,
        external_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        self._validate_authorization(client)
        self._validate_migration_mappings_exists(client)
        external_ids = external_ids or InteractiveCanvasSelect(client).select_external_ids()
        if external_ids is None or not external_ids:
            self.console("No canvases selected for migration.")
            return
        action = "Would migrate" if dry_run else "Migrating"
        self.console(f"{action} {len(external_ids)} canvases.")
        for external_id in external_ids:
            self._migrate_canvas(client, external_id, dry_run=dry_run, verbose=verbose)

    @classmethod
    def _validate_authorization(cls, client: ToolkitClient) -> None:
        required_capabilities: list[Capability] = [
            DataModelsAcl(
                actions=[DataModelsAcl.Action.Read],
                scope=SpaceIDScope([cls.canvas_schema_space, MAPPING_VIEW_ID.space]),
            ),
            DataModelInstancesAcl(
                actions=[
                    DataModelInstancesAcl.Action.Read,
                    DataModelInstancesAcl.Action.Write,
                    DataModelInstancesAcl.Action.Write_Properties,
                ],
                scope=SpaceIDScope([CANVAS_INSTANCE_SPACE]),
            ),
        ]
        if missing := client.iam.verify_capabilities(required_capabilities):
            raise AuthenticationError(f"Missing required capabilities: {humanize_collection(missing)}.")

    @classmethod
    def _validate_migration_mappings_exists(cls, client: ToolkitClient) -> None:
        view = client.data_modeling.views.retrieve(MAPPING_VIEW_ID)
        if not view:
            raise ToolkitMigrationError(
                f"The migration mapping view {MAPPING_VIEW_ID} does not exist. "
                f"Please run the `cdf migrate prepare` command to deploy the migration data model."
            )

    def _migrate_canvas(
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
        if any(
            ref.container_reference_type in {"asset", "event", "file", "timeseries"}
            for ref in canvas.container_references
        ):
            self.warn(
                LowSeverityWarning(
                    f"Canvas with name '{canvas.canvas.name}' does not have any asset-centric references. Skipping.. "
                )
            )
        if verbose:
            self.console(f"Found canvas: {canvas.canvas.name}")
        reference_ids = [
            ref.as_asset_centric_id()
            for ref in canvas.container_references
            if ref.container_reference_type in {"asset", "event", "file", "timeseries"}
        ]
        mappings = client.migration.mapping.retrieve(reference_ids)
        mapping_by_reference_id = {mapping.as_asset_centric_id(): mapping for mapping in mappings}
        missing = set(reference_ids) - set(mapping_by_reference_id.keys())
        if missing:
            self.warn(
                HighSeverityWarning(
                    f"Canvas '{canvas.canvas.name}' has references to resources that are not been migrated: {humanize_collection(missing)}. Skipping.. "
                )
            )
            return
        if dry_run:
            self.console(
                f"Canvas '{canvas.canvas.name}' is ready for migration all {len(mappings)} references asset-centric resources found."
            )
            return
        if verbose:
            self.console(
                f"Migrating canvas '{canvas.canvas.name}' with {len(mappings)} references to asset-centric resources."
            )
        backup = canvas.as_write().create_backup()
        update = canvas.as_write()
        to_migrate = [
            ref
            for ref in update.container_references
            if ref.container_reference_type in {"asset", "event", "file", "timeseries"}
        ]
        update.container_references = [
            ref
            for ref in update.container_references
            if ref.container_reference_type not in {"asset", "event", "file", "timeseries"}
        ]
        for ref in to_migrate:
            mapping = mapping_by_reference_id[ref.as_asset_centric_id()]
            fdm_ref = self.migrate_container_reference(ref, mapping)
            update.fdm_instance_container_references.append(fdm_ref)

        _ = client.canvas.industrial.update(update)
        client.canvas.industrial.create(backup)
        self.console(
            f'Canvas "{canvas.canvas.name}" migrated successfully with {len(to_migrate)} references to data model instances.'
        )

    @classmethod
    def migrate_container_reference(
        cls, reference: ContainerReferenceApply, mapping: Mapping
    ) -> FdmInstanceContainerReferenceApply:
        """Migrate a single container reference by replacing the asset-centric ID with the data model instance ID."""
        return FdmInstanceContainerReferenceApply(
            external_id=reference.external_id,
            id_=reference.id_,
            container_reference_type="fdmInstance",
            instance_space=mapping.space,
            instance_external_id=mapping.external_id,
            # For now, we just map into CogniteCore. Ideally, we should map to an extension view
            # as that is likely what the user wants. However, this requires us to store which view to use for
            # each instance
            view_space="cdf_cdm",
            view_external_id=mapping.default_core_view_external_id,
            view_version="v1",
            label=reference.label,
            properties_=reference.properties_,
            x=reference.x,
            y=reference.y,
            width=reference.width,
            height=reference.height,
            max_width=reference.max_width,
            max_height=reference.max_height,
        )
