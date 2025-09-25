from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.deploy import DeployCommand
from cognite_toolkit._cdf_tk.cruds import (
    ContainerCRUD,
    DataModelCRUD,
    ResourceViewMappingCRUD,
    ResourceWorker,
    SpaceCRUD,
    ViewCRUD,
)
from cognite_toolkit._cdf_tk.data_classes import DeployResults

from .data_model import COGNITE_MIGRATION_MODEL, CONTAINERS, MODEL_ID, SPACE, VIEWS
from .default_mappings import create_default_mappings


class MigrationPrepareCommand(ToolkitCommand):
    def deploy_cognite_migration(self, client: ToolkitClient, dry_run: bool, verbose: bool = False) -> DeployResults:
        """Deploys the Cognite Migration Data Model"""

        deploy_cmd = DeployCommand(self.print_warning, silent=self.silent)
        deploy_cmd.tracker = self.tracker

        verb = "Would deploy" if dry_run else "Deploying"
        print(f"{verb} {MODEL_ID!r}")
        results = DeployResults([], "deploy", dry_run=dry_run)
        for loader_cls, resource_list in [
            (SpaceCRUD, [SPACE]),
            (ContainerCRUD, CONTAINERS),
            (ViewCRUD, VIEWS),
            (DataModelCRUD, [COGNITE_MIGRATION_MODEL]),
            (ResourceViewMappingCRUD, create_default_mappings()),
        ]:
            # MyPy does not understand that `loader_cls` has a `create_loader` method.
            loader = loader_cls.create_loader(client)  # type: ignore[attr-defined]
            worker = ResourceWorker(loader, "deploy")
            # MyPy does not understand that `loader` has a `get_id` method.
            dump_arg = {"context": "local"} if loader_cls is ResourceViewMappingCRUD else {}
            local_by_id = {loader.get_id(item): (item.dump(**dump_arg), item) for item in resource_list}  # type: ignore[attr-defined]
            worker.validate_access(local_by_id, is_dry_run=dry_run)
            cdf_resources = loader.retrieve(list(local_by_id.keys()))
            resources = worker.categorize_resources(local_by_id, cdf_resources, False, verbose)

            if dry_run:
                result = deploy_cmd.dry_run_deploy(resources, loader, False, False)
            else:
                result = deploy_cmd.actual_deploy(resources, loader)
            if result:
                results[result.name] = result
        if results.has_counts:
            print(results.counts_table())
        return results
