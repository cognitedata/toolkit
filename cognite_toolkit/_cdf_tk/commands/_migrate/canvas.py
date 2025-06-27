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

        print(f"Would migrate {len(external_ids)} canvases: {humanize_collection(external_ids)}")

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
