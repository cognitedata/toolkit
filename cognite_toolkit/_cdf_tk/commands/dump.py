from __future__ import annotations

import itertools
from pathlib import Path

import questionary
from cognite.client import data_modeling as dm
from cognite.client.data_classes.capabilities import DataModelsAcl
from cognite.client.data_classes.data_modeling import DataModelId
from questionary import Choice
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.loaders import ViewLoader
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, safe_write, yaml_safe_dump

from ._base import ToolkitCommand


class DumpCommand(ToolkitCommand):
    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        selected_data_model: DataModelId | None,
        output_dir: Path,
        clean: bool,
        verbose: bool,
    ) -> None:
        if selected_data_model is None:
            data_model_id = self._interactive_select_data_model(ToolGlobals, include_global=False)
        else:
            data_model_id = selected_data_model

        print(f"Dumping {data_model_id} from project {ToolGlobals.project}...")
        client = ToolGlobals.toolkit_client
        print("Verifying access rights...")
        if missing := client.verify.authorization(
            DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.All())
        ):
            raise client.verify.create_error(missing, "dumping data model")

        data_models = client.data_modeling.data_models.retrieve(data_model_id, inline_views=True)
        if not data_models:
            raise ToolkitMissingResourceError(f"Data model {data_model_id} does not exist")

        data_model = data_models.latest_version()
        views = dm.ViewList(data_model.views)

        container_ids = views.referenced_containers()

        containers = client.data_modeling.containers.retrieve(list(container_ids))
        space_ids = {item.space for item in itertools.chain(containers, views, [data_model])}
        spaces = client.data_modeling.spaces.retrieve(list(space_ids))

        is_populated = output_dir.exists() and any(output_dir.iterdir())
        if is_populated and clean:
            safe_rmtree(output_dir)
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
            safe_write(space_file, space.as_write().dump_yaml())
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped space {space.space} to {space_file!s}.")

        prefix_space = len(containers) != len({container.external_id for container in containers})
        container_folder = resource_folder / "containers"
        container_folder.mkdir(exist_ok=True)
        for container in containers:
            file_name = f"{container.external_id}.container.yaml"
            if prefix_space:
                file_name = f"{container.space}_{file_name}"
            container_file = container_folder / file_name
            safe_write(container_file, container.as_write().dump_yaml())
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped container {container.external_id} to {container_file!s}.")

        prefix_space = len(views) != len({view.external_id for view in views})
        suffix_version = len(views) != len({f"{view.space}{view.external_id}" for view in views})
        view_folder = resource_folder / "views"
        view_folder.mkdir(exist_ok=True)
        view_loader = ViewLoader.create_loader(ToolGlobals, None)
        for view in views:
            file_name = f"{view.external_id}.view.yaml"
            if prefix_space:
                file_name = f"{view.space}_{file_name}"
            if suffix_version:
                file_name = f"{file_name.removesuffix('.view.yaml')}_{view.version}.view.yaml"
            view_file = view_folder / file_name
            view_write = view_loader.dump_as_write(view)
            safe_write(view_file, yaml_safe_dump(view_write))
            if verbose:
                print(f"  [bold green]INFO:[/] Dumped view {view.as_id()} to {view_file!s}.")

        data_model_file = resource_folder / f"{data_model.external_id}.datamodel.yaml"
        data_model_write = data_model.as_write()
        data_model_write.views = views.as_ids()  # type: ignore[assignment]
        safe_write(data_model_file, data_model_write.dump_yaml())

        print(Panel(f"Dumped {data_model_id} to {resource_folder!s}", title="Success", style="green"))

    def _interactive_select_data_model(self, ToolGlobals: CDFToolConfig, include_global: bool = False) -> DataModelId:
        spaces = ToolGlobals.toolkit_client.data_modeling.spaces.list(limit=-1, include_global=include_global)
        selected_space: str = questionary.select(
            "In which space is your data model located?", [space.space for space in spaces]
        ).ask()

        data_models = ToolGlobals.toolkit_client.data_modeling.data_models.list(
            space=selected_space, all_versions=False, limit=-1, include_global=include_global
        ).as_ids()

        if not data_models:
            raise ToolkitMissingResourceError(f"No data models found in space {selected_space}")

        selected_data_model: DataModelId = questionary.select(
            "Which data model would you like to dump?", [Choice(f"{model!r}", value=model) for model in data_models]
        ).ask()

        data_models = ToolGlobals.toolkit_client.data_modeling.data_models.list(
            space=selected_space,
            all_versions=True,
            limit=-1,
            include_global=include_global,
        ).as_ids()
        data_model_versions = [
            model.version
            for model in data_models
            if (model.space, model.external_id) == (selected_data_model.space, selected_data_model.external_id)
            and model.version is not None
        ]
        if (
            len(data_model_versions) == 1
            or not questionary.confirm(
                f"Would you like to select a different version than {selected_data_model.version} of the data model",
                default=False,
            ).ask()
        ):
            return selected_data_model

        selected_version = questionary.select("Which version would you like to dump?", data_model_versions).ask()
        return DataModelId(selected_space, selected_data_model.external_id, selected_version)
