from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import (
    DeploymentResult,
    DeploymentStep,
    DeployOptions,
    DeployV2Command,
)
from cognite_toolkit._cdf_tk.resource_ios import (
    ContainerCRUD,
    DataModelIO,
    ResourceViewMappingIO,
    SpaceCRUD,
    ViewIO,
)

from .data_model import COGNITE_MIGRATION_MODEL, CONTAINERS, MODEL_ID, SPACE, VIEWS
from .default_mappings import create_default_mappings


class MigrationPrepareCommand(ToolkitCommand):
    def deploy_cognite_migration(
        self, client: ToolkitClient, dry_run: bool, verbose: bool = False
    ) -> Sequence[DeploymentResult]:
        """Deploys the Cognite Migration Data Model"""

        deploy_cmd = DeployV2Command(self.print_warning, silent=self.silent)
        deploy_cmd.tracker = self.tracker
        verb = "Would deploy" if dry_run else "Deploying"
        self.console(f"{verb} {MODEL_ID!r}")

        plan: list[DeploymentStep[Any]] = [
            DeploymentStep(SpaceCRUD, [], resource_requests=[SPACE]),
            DeploymentStep(ContainerCRUD, [], resource_requests=CONTAINERS),
            DeploymentStep(ViewIO, [], resource_requests=VIEWS),
            DeploymentStep(DataModelIO, [], resource_requests=[COGNITE_MIGRATION_MODEL]),
            DeploymentStep(ResourceViewMappingIO, [], resource_requests=create_default_mappings()),
        ]

        results = deploy_cmd.apply_plan(
            client, plan, options=DeployOptions(operation="deploy", dry_run=dry_run, verbose=verbose)
        )
        deploy_cmd._display_results(results, "deploy", console=client.console, verbose=verbose)
        return results
