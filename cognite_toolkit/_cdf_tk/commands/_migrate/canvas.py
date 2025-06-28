from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl, DataModelsAcl, SpaceIDScope
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    Canvas,
    ContainerReferenceApply,
    FdmInstanceContainerReferenceApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.migration import Mapping, AssetCentricId
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning
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
        # 1. Fetch canvas with all its connections, including implicit.
        # 2a. Try to download mappings for the resources reference in the canvas.
        # 2b. If not found, Warning and skip.
        # 3. If dry_run, log all good and go to next.
        # 4. If not dry_run, create a new canvas version with updated mappings and connections.
        # 5. Deploy new version of the canvas. (Include rollback if something goes wrong?)
        canvas = client.canvas.industrial.retrieve(external_id=external_id)
        if not canvas:
            self.warn(MediumSeverityWarning(f"Canvas with external ID '{external_id}' not found. Skipping.. "))
            return
        if canvas.fdm_instance_container_references:
            self.warn(
                MediumSeverityWarning(
                    f"Canvas with name '{canvas.canvas.name}' already has references to data model instances. Skipping.. "
                )
            )
        if verbose:
            self.console(f"Found canvas: {canvas.canvas.name}")
        reference_ids = [ref.as_asset_centric_id() for ref in canvas.container_references]
        mappings = client.migration.mapping.retrieve(reference_ids)
        mapping_by_reference_id = {mapping.as_asset_centric_id(): mapping for mapping in mappings}
        missing = set(reference_ids) - set(mapping_by_reference_id.keys())
        if missing:
            self.warn(
                MediumSeverityWarning(
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
        new_canvas = canvas.as_write().duplicate()
        new_canvas.fdm_instance_container_references = [
            self._migrate_container_reference(ref, mapping_by_reference_id) for ref in new_canvas.container_references
        ]
        new_canvas.container_references = []
        try:
            _ = client.canvas.industrial.new_version(new_canvas, canvas)
        except CogniteAPIError as e:
            client.canvas.industrial.delete(new_canvas)
            self.warn(HighSeverityWarning(f"Failed to create new version of canvas '{canvas.canvas.name}': {e}"))
        self.console(
            f'Canvas "{canvas.canvas.name}" migrated successfully with {len(new_canvas.fdm_instance_container_references)} references to data model instances.'
        )

    def _migrate_container_reference(
        self, reference: ContainerReferenceApply, mapping_by_reference_id: dict[AssetCentricId, Mapping]
    ) -> FdmInstanceContainerReferenceApply:
        """Migrate a single container reference by replacing the asset-centric ID with the data model instance ID."""
        raise NotImplementedError()
