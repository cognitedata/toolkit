from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable, Iterator
from pathlib import Path
from typing import Generic, cast

import questionary
import typer
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    Group,
    GroupList,
    Transformation,
    TransformationList,
    TransformationNotificationList,
    TransformationScheduleList,
)
from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling import DataModelId
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowList,
    WorkflowTriggerList,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
)
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceRetrievalError,
    ToolkitMissingResourceError,
    ToolkitResourceMissingError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import (
    ContainerLoader,
    DataModelLoader,
    GroupLoader,
    NodeLoader,
    ResourceLoader,
    SpaceLoader,
    TransformationLoader,
    TransformationNotificationLoader,
    TransformationScheduleLoader,
    ViewLoader,
    WorkflowLoader,
    WorkflowTriggerLoader,
    WorkflowVersionLoader,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID
from cognite_toolkit._cdf_tk.tk_warnings import FileExistsWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, safe_write, yaml_safe_dump

from ._base import ToolkitCommand


class ResourceFinder(Iterable, ABC, Generic[T_ID]):
    def __init__(self, client: ToolkitClient, identifier: T_ID | None = None):
        self.client = client
        self.identifier = identifier

    def _selected(self) -> T_ID:
        return self.identifier or self._interactive_select()

    @abstractmethod
    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        raise NotImplementedError

    @abstractmethod
    def _interactive_select(self) -> T_ID:
        raise NotImplementedError

    # Can be implemented in subclasses
    def update(self, resources: CogniteResourceList) -> None: ...


class DataModelFinder(ResourceFinder[DataModelId]):
    def __init__(self, client: ToolkitClient, identifier: DataModelId | None = None, include_global: bool = False):
        super().__init__(client, identifier)
        self._include_global = include_global
        self.data_model: dm.DataModel[dm.ViewId] | None = None
        self.view_ids: set[dm.ViewId] = set()
        self.container_ids: set[dm.ContainerId] = set()
        self.space_ids: set[str] = set()

    def _interactive_select(self) -> DataModelId:
        data_model_ids = self.client.data_modeling.data_models.list(
            all_versions=False, limit=-1, include_global=False
        ).as_ids()
        available_spaces = sorted({model.space for model in data_model_ids})
        if not available_spaces:
            raise ToolkitMissingResourceError("No data models found")
        if len(available_spaces) == 1:
            selected_space = available_spaces[0]
        else:
            selected_space = questionary.select("In which space is your data model located?", available_spaces).ask()
        data_model_ids = sorted(
            [model for model in data_model_ids if model.space == selected_space], key=lambda model: model.as_tuple()
        )

        selected_data_model: DataModelId = questionary.select(
            "Which data model would you like to dump?",
            [
                Choice(f"{model_id!r}", value=model_id)
                for model_id in sorted(data_model_ids, key=lambda model: model.as_tuple())
            ],
        ).ask()

        retrieved_models = self.client.data_modeling.data_models.retrieve(
            (selected_data_model.space, selected_data_model.external_id), inline_views=False
        )
        if not retrieved_models:
            # This happens if the data model is removed after the list call above.
            raise ToolkitMissingResourceError(f"Data model {selected_data_model} not found")
        if len(retrieved_models) == 1:
            self.data_model = retrieved_models[0]
            return selected_data_model
        models_by_version = {model.version: model for model in retrieved_models if model.version is not None}
        if len(models_by_version) == 1:
            self.data_model = retrieved_models[0]
            return selected_data_model
        if not questionary.confirm(
            f"Would you like to select a different version than {selected_data_model.version} of the data model",
            default=False,
        ).ask():
            self.data_model = models_by_version[cast(str, selected_data_model.version)]
            return selected_data_model

        selected_model = questionary.select(
            "Which version would you like to dump?",
            [Choice(f"{version}", value=model) for version, model in models_by_version.items()],
        ).ask()
        self.data_model = models_by_version[selected_model]
        return self.data_model.as_id()

    def update(self, resources: CogniteResourceList) -> None:
        if isinstance(resources, dm.DataModelList):
            self.view_ids |= {
                view.as_id() if isinstance(view, dm.View) else view for item in resources for view in item.views
            }
        elif isinstance(resources, dm.ViewList):
            self.container_ids |= resources.referenced_containers()
        elif isinstance(resources, dm.SpaceList):
            return
        self.space_ids |= {item.space for item in resources}

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        self.identifier = self._selected()
        model_loader = DataModelLoader.create_loader(self.client)
        if self.data_model:
            is_global_model = self.data_model.is_global
            yield [], dm.DataModelList([self.data_model]), model_loader, None
        else:
            model_list = model_loader.retrieve([self.identifier])
            if not model_list:
                raise ToolkitResourceMissingError(f"Data model {self.identifier} not found", str(self.identifier))
            is_global_model = model_list[0].is_global
            yield [], model_list, model_loader, None
        if self._include_global or is_global_model:
            yield list(self.view_ids), None, ViewLoader.create_loader(self.client), "views"
            yield list(self.container_ids), None, ContainerLoader.create_loader(self.client), "containers"
            yield list(self.space_ids), None, SpaceLoader.create_loader(self.client), None
        else:
            view_loader = ViewLoader.create_loader(self.client)
            views = dm.ViewList([view for view in view_loader.retrieve(list(self.view_ids)) if not view.is_global])
            yield [], views, view_loader, "views"
            container_loader = ContainerLoader.create_loader(self.client)
            containers = dm.ContainerList(
                [
                    container
                    for container in container_loader.retrieve(list(self.container_ids))
                    if not container.is_global
                ]
            )
            yield [], containers, container_loader, "containers"

            space_loader = SpaceLoader.create_loader(self.client)
            spaces = dm.SpaceList(
                [space for space in space_loader.retrieve(list(self.space_ids)) if not space.is_global]
            )
            yield [], spaces, space_loader, None


class WorkflowFinder(ResourceFinder[WorkflowVersionId]):
    def __init__(self, client: ToolkitClient, identifier: WorkflowVersionId | None = None):
        super().__init__(client, identifier)
        self._workflow: Workflow | None = None
        self._workflow_version: WorkflowVersion | None = None

    def _interactive_select(self) -> WorkflowVersionId:
        workflows = self.client.workflows.list(limit=-1)
        if not workflows:
            raise ToolkitMissingResourceError("No workflows found")
        selected_workflow_id: str = questionary.select(
            "Which workflow would you like to dump?",
            [Choice(workflow_id, value=workflow_id) for workflow_id in workflows.as_external_ids()],
        ).ask()
        for workflow in workflows:
            if workflow.external_id == selected_workflow_id:
                self._workflow = workflow
                break

        versions = self.client.workflows.versions.list(selected_workflow_id, limit=-1)
        if len(versions) == 0:
            raise ToolkitMissingResourceError(f"No versions found for workflow {selected_workflow_id}")
        if len(versions) == 1:
            self._workflow_version = versions[0]
            return self._workflow_version.as_id()

        selected_version: WorkflowVersionId = questionary.select(
            "Which version would you like to dump?",
            [Choice(f"{version!r}", value=version) for version in versions.as_ids()],
        ).ask()
        for version in versions:
            if version.version == selected_version.version:
                self._workflow_version = version
                break
        return selected_version

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        self.identifier = self._selected()
        if self._workflow:
            yield [], WorkflowList([self._workflow]), WorkflowLoader.create_loader(self.client), None
        else:
            yield [self.identifier.workflow_external_id], None, WorkflowLoader.create_loader(self.client), None
        if self._workflow_version:
            yield (
                [],
                WorkflowVersionList([self._workflow_version]),
                WorkflowVersionLoader.create_loader(self.client),
                None,
            )
        else:
            yield [self.identifier], None, WorkflowVersionLoader.create_loader(self.client), None
        trigger_loader = WorkflowTriggerLoader.create_loader(self.client)
        trigger_list = WorkflowTriggerList(
            list(trigger_loader.iterate(parent_ids=[self.identifier.workflow_external_id]))
        )
        yield [], trigger_list, trigger_loader, None


class TransformationFinder(ResourceFinder[str]):
    def __init__(self, client: ToolkitClient, identifier: str | None = None):
        super().__init__(client, identifier)
        self.transformation: Transformation | None = None

    def _interactive_select(self) -> str:
        transformations = self.client.transformations.list(limit=-1)
        transformation_ids = [
            transformation.external_id for transformation in transformations if transformation.external_id
        ]

        if transformations and not transformation_ids:
            raise ToolkitValueError(
                "ExternalID is required for dumping transformations. "
                f"Found {len(transformations)} transformations with only internal IDs."
            )
        elif not transformation_ids:
            raise ToolkitMissingResourceError("No transformations found")

        selected_transformation_id: str = questionary.select(
            "Which transformation would you like to dump?",
            [
                Choice(transformation.external_id, value=transformation.external_id)
                for transformation in transformations
                if transformation.external_id
            ],
        ).ask()
        for transformation in transformations:
            if transformation.external_id == selected_transformation_id:
                self.transformation = transformation
                break

        return selected_transformation_id

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        self.identifier = self._selected()
        if self.transformation:
            yield [], TransformationList([self.transformation]), TransformationLoader.create_loader(self.client), None
        else:
            yield [self.identifier], None, TransformationLoader.create_loader(self.client), None

        schedule_loader = TransformationScheduleLoader.create_loader(self.client)
        schedule_list = TransformationScheduleList(schedule_loader.iterate(parent_ids=[self.identifier]))
        yield [], schedule_list, schedule_loader, None
        notification_loader = TransformationNotificationLoader.create_loader(self.client)
        notification_list = TransformationNotificationList(notification_loader.iterate(parent_ids=[self.identifier]))
        yield [], notification_list, notification_loader, None


class GroupFinder(ResourceFinder[str]):
    def __init__(self, client: ToolkitClient, identifier: str | None = None):
        super().__init__(client, identifier)
        self.group: Group | None = None

    def _interactive_select(self) -> str:
        groups = self.client.iam.groups.list(all=True)
        if not groups:
            raise ToolkitMissingResourceError("No groups found")
        group_names = [group.name for group in groups]
        selected_group_name: str = questionary.select(
            "Which group would you like to dump?",
            [Choice(group, value=group) for group in group_names],
        ).ask()
        for group in groups:
            if group.name == selected_group_name:
                self.group = group
                break
        return selected_group_name

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        self.identifier = self._selected()
        if self.group:
            yield [], GroupList([self.group]), GroupLoader.create_loader(self.client), None
        else:
            yield [self.identifier], None, GroupLoader.create_loader(self.client), None


class NodeFinder(ResourceFinder[dm.ViewId]):
    def __init__(self, client: ToolkitClient, identifier: dm.ViewId | None = None):
        super().__init__(client, identifier)
        self.is_interactive = False

    def _interactive_select(self) -> dm.ViewId:
        self.is_interactive = True
        spaces = self.client.data_modeling.spaces.list(limit=-1)
        if not spaces:
            raise ToolkitMissingResourceError("No spaces found")
        selected_space: str = questionary.select(
            "In which space is your node property view located?", [space.space for space in spaces]
        ).ask()

        views = self.client.data_modeling.views.list(space=selected_space, limit=-1, all_versions=False)
        if not views:
            raise ToolkitMissingResourceError(f"No views found in {selected_space}")
        if len(views) == 1:
            return views[0].as_id()
        selected_view_id: dm.ViewId = questionary.select(
            "Which node property view would you like to dump?",
            [Choice(repr(view), value=view) for view in views.as_ids()],
        ).ask()
        return selected_view_id

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceLoader, None | str]]:
        self.identifier = self._selected()
        loader = NodeLoader(self.client, None, None, self.identifier)
        if self.is_interactive:
            count = self.client.data_modeling.instances.aggregate(
                self.identifier, dm.aggregations.Count("externalId"), instance_type="node"
            ).value
            if count == 0 or count is None:
                raise ToolkitMissingResourceError(f"No nodes found in {self.identifier}")
            elif count > 50:
                if not questionary.confirm(
                    f"Are you sure you want to dump {count} nodes? This may take a while.",
                    default=False,
                ).ask():
                    typer.Exit(0)
        nodes = dm.NodeList[dm.Node](list(loader.iterate()))
        yield [], nodes, loader, None


class DumpResourceCommand(ToolkitCommand):
    def dump_to_yamls(
        self,
        finder: ResourceFinder,
        output_dir: Path,
        clean: bool,
        verbose: bool,
    ) -> None:
        is_populated = output_dir.exists() and any(output_dir.iterdir())
        if is_populated and clean:
            safe_rmtree(output_dir)
            output_dir.mkdir()
            self.console(f"Cleaned existing output directory {output_dir!s}.")
        elif is_populated:
            self.warn(MediumSeverityWarning("Output directory is not empty. Use --clean to remove existing files."))
        elif not output_dir.exists():
            output_dir.mkdir(exist_ok=True)

        for identifiers, resources, loader, subfolder in finder:
            if not identifiers and not resources:
                # No resources to dump
                continue
            if resources is None:
                try:
                    resources = loader.retrieve(identifiers)
                except CogniteAPIError as e:
                    raise ResourceRetrievalError(f"Failed to retrieve {humanize_collection(identifiers)}: {e!s}") from e
                if len(resources) == 0:
                    raise ToolkitResourceMissingError(
                        f"Resource(s) {humanize_collection(identifiers)} not found", str(identifiers)
                    )

            finder.update(resources)
            resource_folder = output_dir / loader.folder_name
            if subfolder:
                resource_folder = resource_folder / subfolder
            resource_folder.mkdir(exist_ok=True, parents=True)
            for resource in resources:
                name = loader.as_str(loader.get_id(resource))
                base_filepath = resource_folder / f"{name}.{loader.kind}.yaml"
                if base_filepath.exists():
                    self.warn(FileExistsWarning(base_filepath, "Skipping... Use --clean to remove existing files."))
                    continue
                dumped = loader.dump_resource(resource)
                for filepath, subpart in loader.split_resource(base_filepath, dumped):
                    content = subpart if isinstance(subpart, str) else yaml_safe_dump(subpart)
                    safe_write(filepath, content, encoding="utf-8")
                    if verbose:
                        self.console(f"Dumped {loader.kind} {name} to {filepath!s}")

        print(Panel(f"Dumped {finder.identifier}", title="Success", style="green", expand=False))
