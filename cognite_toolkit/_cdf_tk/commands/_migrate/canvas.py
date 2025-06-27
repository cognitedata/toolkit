from cognite.client.data_classes import filters
from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl, DataModelsAcl, SpaceIDScope
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import CANVAS_INSTANCE_SPACE, Canvas
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitMigrationError
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
            print("No canvases selected for migration.")
            return
        action = "Would migrate" if dry_run else "Migrating"
        print(f"{action} {len(external_ids)} canvases.")
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

        canvas = client.canvas.retrieve(external_id=external_id)
        if not canvas:
            # Todo Warning
            print(f"Canvas with external ID '{external_id}' not found.")
            return
        if verbose:
            print(f"Found canvas: {canvas.name}")
        node_id = canvas.as_id()
        is_canvas = filters.Equals(["edge", "startNode"], node_id.dump(include_instance_type=False))
        edges = client.data_modeling.instances.list(instance_type="edge", space=CANVAS_INSTANCE_SPACE, filter=is_canvas)
