import itertools
import shutil
from pathlib import Path

import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes.capabilities import DataModelsAcl
from cognite.client.data_classes.data_modeling import DataModelId
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, retrieve_view_ancestors

from ._base import ToolkitCommand


class DumpCommand(ToolkitCommand):
    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        data_model_id: DataModelId,
        output_dir: Path,
        clean: bool,
        verbose: bool,
    ) -> None:
        print(f"Dumping {data_model_id} from project {ToolGlobals.project}...")
        print("Verifying access rights...")
        client = ToolGlobals.verify_authorization(
            DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.All()),
        )

        data_models = client.data_modeling.data_models.retrieve(data_model_id, inline_views=True)
        if not data_models:
            raise ToolkitMissingResourceError(f"Data model {data_model_id} does not exist")

        data_model = data_models.latest_version()
        views = dm.ViewList(data_model.views)

        container_ids = views.referenced_containers()

        containers = client.data_modeling.containers.retrieve(list(container_ids))
        space_ids = {item.space for item in itertools.chain(containers, views, [data_model])}
        spaces = client.data_modeling.spaces.retrieve(list(space_ids))

        views_by_id = {view.as_id(): view for view in views}

        is_populated = output_dir.exists() and any(output_dir.iterdir())
        if is_populated and clean:
            shutil.rmtree(output_dir)
            output_dir.mkdir()
            print(f"  [bold green]INFO:[/] Cleaned existing output directory {output_dir!s}.")
        elif is_populated:
            self.warn(MediumSeverityWarning("Output directory is not empty. Use --clean to remove existing files."))
        elif not output_dir.exists():
            output_dir.mkdir(exist_ok=True)

        resource_folder = output_dir / "data_models"
        resource_folder.mkdir(exist_ok=True)
        for space in spaces:
            space_file = resource_folder / f"{space.space}.space.yaml"
            space_file.write_text(space.as_write().dump_yaml())
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped space {space.space} to {space_file!s}.")

        prefix_space = len(containers) != len({container.external_id for container in containers})
        for container in containers:
            file_name = f"{container.external_id}.container.yaml"
            if prefix_space:
                file_name = f"{container.space}_{file_name}"
            container_file = resource_folder / file_name
            container_file.write_text(container.as_write().dump_yaml())
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped container {container.external_id} to {container_file!s}.")

        prefix_space = len(views) != len({view.external_id for view in views})
        suffix_version = len(views) != len({f"{view.space}{view.external_id}" for view in views})
        for view in views:
            file_name = f"{view.external_id}.view.yaml"
            if prefix_space:
                file_name = f"{view.space}_{file_name}"
            if suffix_version:
                file_name = f"{file_name.removesuffix('.view.yaml')}_{view.version}.view.yaml"
            view_file = resource_folder / file_name
            view_write = view.as_write().dump()
            parents = retrieve_view_ancestors(client, view.implements or [], views_by_id)
            for parent in parents:
                for prop_name in parent.properties.keys():
                    view_write["properties"].pop(prop_name, None)
            if not view_write["properties"]:
                # All properties were removed, so we remove the properties key.
                view_write.pop("properties", None)

            view_file.write_text(yaml.safe_dump(view_write, sort_keys=False))
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped view {view.as_id()} to {view_file!s}.")

        data_model_file = resource_folder / f"{data_model.external_id}.datamodel.yaml"
        data_model_write = data_model.as_write()
        data_model_write.views = views.as_ids()  # type: ignore[assignment]
        data_model_file.write_text(data_model_write.dump_yaml())

        print(Panel(f"Dumped {data_model_id} to {resource_folder!s}", title="Success", style="green"))
