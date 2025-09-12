import io
import json
import zipfile
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Hashable, Iterable, Iterator
from functools import cached_property
from pathlib import Path
from typing import Generic, cast

import questionary
import typer
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    DataSetList,
    ExtractionPipelineList,
    Group,
    GroupList,
    TransformationList,
    TransformationNotificationList,
    TransformationScheduleList,
    filters,
)
from cognite.client.data_classes._base import (
    CogniteResourceList,
)
from cognite.client.data_classes.agents import (
    AgentList,
)
from cognite.client.data_classes.data_modeling import DataModelId
from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineConfigList
from cognite.client.data_classes.functions import (
    Function,
    FunctionList,
    FunctionSchedulesList,
)
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowList,
    WorkflowTriggerList,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import ms_to_datetime
from questionary import Choice
from rich import print
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilterList
from cognite_toolkit._cdf_tk.client.data_classes.streamlit_ import Streamlit, StreamlitList
from cognite_toolkit._cdf_tk.cruds import (
    AgentCRUD,
    ContainerCRUD,
    DataModelCRUD,
    DataSetsCRUD,
    ExtractionPipelineConfigCRUD,
    ExtractionPipelineCRUD,
    FunctionCRUD,
    FunctionScheduleCRUD,
    GroupCRUD,
    LocationFilterCRUD,
    NodeCRUD,
    ResourceCRUD,
    SpaceCRUD,
    StreamlitCRUD,
    TransformationCRUD,
    TransformationNotificationCRUD,
    TransformationScheduleCRUD,
    ViewCRUD,
    WorkflowCRUD,
    WorkflowTriggerCRUD,
    WorkflowVersionCRUD,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceRetrievalError,
    ToolkitMissingResourceError,
    ToolkitResourceMissingError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import FileExistsWarning, HighSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, safe_write, to_directory_compatible, yaml_safe_dump

from ._base import ToolkitCommand


class ResourceFinder(Iterable, ABC, Generic[T_ID]):
    def __init__(self, client: ToolkitClient, identifier: T_ID | None = None):
        self.client = client
        self.identifier = identifier

    def _selected(self) -> T_ID:
        return self.identifier or self._interactive_select()

    @abstractmethod
    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
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

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        model_loader = DataModelCRUD.create_loader(self.client)
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
            yield list(self.view_ids), None, ViewCRUD.create_loader(self.client), "views"
            yield list(self.container_ids), None, ContainerCRUD.create_loader(self.client), "containers"
            yield list(self.space_ids), None, SpaceCRUD.create_loader(self.client), None
        else:
            view_loader = ViewCRUD(self.client, None, None, topological_sort_implements=True)
            views = dm.ViewList([view for view in view_loader.retrieve(list(self.view_ids)) if not view.is_global])
            yield [], views, view_loader, "views"
            container_loader = ContainerCRUD.create_loader(self.client)
            containers = dm.ContainerList(
                [
                    container
                    for container in container_loader.retrieve(list(self.container_ids))
                    if not container.is_global
                ]
            )
            yield [], containers, container_loader, "containers"

            space_loader = SpaceCRUD.create_loader(self.client)
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

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        if self._workflow:
            yield [], WorkflowList([self._workflow]), WorkflowCRUD.create_loader(self.client), None
        else:
            yield [self.identifier.workflow_external_id], None, WorkflowCRUD.create_loader(self.client), None
        if self._workflow_version:
            yield (
                [],
                WorkflowVersionList([self._workflow_version]),
                WorkflowVersionCRUD.create_loader(self.client),
                None,
            )
        else:
            yield [self.identifier], None, WorkflowVersionCRUD.create_loader(self.client), None
        trigger_loader = WorkflowTriggerCRUD.create_loader(self.client)
        trigger_list = WorkflowTriggerList(
            list(trigger_loader.iterate(parent_ids=[self.identifier.workflow_external_id]))
        )
        yield [], trigger_list, trigger_loader, None


class TransformationFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.transformations: TransformationList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        self.transformations = self.client.transformations.list(limit=-1)
        if self.transformations and not any(transformation.external_id for transformation in self.transformations):
            raise ToolkitValueError(
                "ExternalID is required for dumping transformations. "
                f"Found {len(self.transformations)} transformations with only internal IDs."
            )
        elif not self.transformations:
            raise ToolkitMissingResourceError("No transformations found")

        choices = [
            Choice(f"{transformation.name} ({transformation.external_id})", value=transformation.external_id)
            for transformation in sorted(self.transformations, key=lambda t: t.name or "")
            if transformation.external_id
        ]

        selected_transformation_ids: tuple[str, ...] | None = questionary.checkbox(
            "Which transformation(s) would you like to dump?",
            choices=choices,
        ).ask()
        if selected_transformation_ids is None:
            raise ToolkitValueError("No transformations selected for dumping.")
        return tuple(selected_transformation_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        if self.transformations:
            yield (
                [],
                TransformationList([t for t in self.transformations if t.external_id in self.identifier]),
                TransformationCRUD.create_loader(self.client),
                None,
            )
        else:
            yield list(self.identifier), None, TransformationCRUD.create_loader(self.client), None

        schedule_loader = TransformationScheduleCRUD.create_loader(self.client)
        schedule_list = TransformationScheduleList(list(schedule_loader.iterate(parent_ids=list(self.identifier))))
        yield [], schedule_list, schedule_loader, None
        notification_loader = TransformationNotificationCRUD.create_loader(self.client)
        notification_list = TransformationNotificationList(
            list(notification_loader.iterate(parent_ids=list(self.identifier)))
        )
        yield [], notification_list, notification_loader, None


class GroupFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.groups: list[Group] | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        groups = self.client.iam.groups.list(all=True)
        if not groups:
            raise ToolkitMissingResourceError("No groups found")
        groups_by_name: dict[str, list[Group]] = defaultdict(list)
        for group in groups:
            groups_by_name[group.name].append(group)
        selected_groups: list[list[Group]] | None = questionary.checkbox(
            "Which group(s) would you like to dump?",
            choices=[
                Choice(f"{group_name} ({len(group_list)} group{'s' if len(group_list) > 1 else ''})", value=group_list)
                for group_name, group_list in sorted(groups_by_name.items())
            ],
        ).ask()
        if not selected_groups:
            raise ToolkitValueError("No group selected for dumping. Aborting...")
        self.groups = [group for group_list in selected_groups for group in group_list]
        return tuple(group_list[0].name for group_list in selected_groups)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        if self.groups:
            yield [], GroupList(self.groups), GroupCRUD.create_loader(self.client), None
        else:
            yield list(self.identifier), None, GroupCRUD.create_loader(self.client), None


class AgentFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.agents: AgentList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        self.agents = self.client.agents.list()
        if not self.agents:
            raise ToolkitMissingResourceError("No agents found")

        choices = [
            Choice(f"{agent.name} ({agent.external_id}) with {len(agent.tools)} tools", value=agent.external_id)
            for agent in sorted(self.agents, key=lambda a: a.name or a.external_id)
            if agent.external_id
        ]

        selected_agent_ids: list[str] | None = questionary.checkbox(
            "Which agent(s) would you like to dump?",
            choices=choices,
        ).ask()
        if selected_agent_ids is None:
            raise ToolkitValueError("No agents selected for dumping.")
        return tuple(selected_agent_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        loader = AgentCRUD.create_loader(self.client)
        if self.agents:
            yield (
                [],
                AgentList([agent for agent in self.agents if agent.external_id in self.identifier]),
                loader,
                None,
            )
        else:
            yield list(self.identifier), None, loader, None


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

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        loader = NodeCRUD(self.client, None, None, self.identifier)
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


class LocationFilterFinder(ResourceFinder[tuple[str, ...]]):
    @cached_property
    def all_filters(self) -> LocationFilterList:
        return self.client.search.locations.list()

    def _interactive_select(self) -> tuple[str, ...]:
        filters = self.all_filters
        if not filters:
            raise ToolkitMissingResourceError("No filters found")
        id_by_display_name = {f"{filter.name} ({filter.external_id})": filter.external_id for filter in filters}
        return tuple(
            questionary.checkbox(
                "Which filters would you like to dump?",
                choices=[Choice(name, value=id_) for name, id_ in id_by_display_name.items()],
            ).ask()
        )

    def _get_filters(self, identifiers: tuple[str, ...]) -> LocationFilterList:
        if not identifiers:
            return self.all_filters
        filters = [f for f in self.all_filters if f.external_id in identifiers]
        if not filters:
            raise ToolkitResourceMissingError(f"Location filters {identifiers} not found", str(identifiers))
        return LocationFilterList(filters)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self.identifier or self._interactive_select()
        filters = self._get_filters(self.identifier)
        yield [], filters, LocationFilterCRUD.create_loader(self.client), None


class ExtractionPipelineFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.extraction_pipelines: ExtractionPipelineList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        self.extraction_pipelines = self.client.extraction_pipelines.list(limit=-1)
        if not self.extraction_pipelines:
            raise ToolkitMissingResourceError("No extraction pipelines found")
        choices = [
            Choice(f"{pipeline.name} ({pipeline.external_id})", value=pipeline.external_id)
            for pipeline in sorted(self.extraction_pipelines, key=lambda p: p.name or "")
            if pipeline.external_id
        ]
        selected_pipeline_ids: tuple[str, ...] | None = questionary.checkbox(
            "Which extraction pipeline(s) would you like to dump?",
            choices=choices,
        ).ask()
        if selected_pipeline_ids is None:
            raise ToolkitValueError("No extraction pipelines selected for dumping.")
        return tuple(selected_pipeline_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        pipeline_loader = ExtractionPipelineCRUD.create_loader(self.client)
        if self.extraction_pipelines:
            selected_pipelines = ExtractionPipelineList(
                [p for p in self.extraction_pipelines if p.external_id in self.identifier]
            )
            yield [], selected_pipelines, pipeline_loader, None
        else:
            yield list(self.identifier), None, pipeline_loader, None
        config_loader = ExtractionPipelineConfigCRUD.create_loader(self.client)
        configs = ExtractionPipelineConfigList(list(config_loader.iterate(parent_ids=list(self.identifier))))
        yield [], configs, config_loader, None


class DataSetFinder(ResourceFinder[tuple[str, ...]]):
    """Finds data sets to dump."""

    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.datasets: DataSetList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        self.datasets = self.client.data_sets.list(limit=-1)
        if not self.datasets:
            raise ToolkitMissingResourceError("No datasets found")
        choices = [
            Choice(f"{dataset.name} ({dataset.external_id})", value=dataset.external_id)
            for dataset in sorted(self.datasets, key=lambda d: d.name or "")
            if dataset.external_id
        ]
        selected_dataset_ids: tuple[str, ...] | None = questionary.checkbox(
            "Which dataset(s) would you like to dump?",
            choices=choices,
        ).ask()
        if selected_dataset_ids is None:
            raise ToolkitValueError("No datasets selected for dumping.")
        return tuple(selected_dataset_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        loader = DataSetsCRUD.create_loader(self.client)
        if self.datasets:
            yield (
                [],
                DataSetList([d for d in self.datasets if d.external_id in set(self.identifier)]),
                loader,
                None,
            )
        else:
            yield list(self.identifier), None, loader, None


class FunctionFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.functions: FunctionList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        self.functions = self.client.functions.list(limit=-1)
        if not self.functions:
            raise ToolkitMissingResourceError("No functions found")
        choices = [
            Choice(f"{function.name} ({function.external_id})", value=function.external_id)
            for function in sorted(self.functions, key=lambda f: f.name or "")
            if function.name and function.external_id
        ]
        selected_function_ids: tuple[str, ...] | None = questionary.checkbox(
            "Which function(s) would you like to dump?",
            choices=choices,
        ).ask()
        if selected_function_ids is None:
            raise ToolkitValueError("No functions selected for dumping.")
        return tuple(selected_function_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        self.identifier = self._selected()
        loader = FunctionCRUD.create_loader(self.client)
        if self.functions:
            selected_functions = FunctionList([f for f in self.functions if f.external_id in self.identifier])
            yield [], selected_functions, loader, None
        else:
            yield list(self.identifier), None, loader, None

        schedule_loader = FunctionScheduleCRUD.create_loader(self.client)
        schedules = schedule_loader.iterate(parent_ids=list(self.identifier))
        yield [], FunctionSchedulesList(list(schedules)), schedule_loader, None

    def dump_function_code(self, function: Function, folder: Path) -> None:
        try:
            zip_bytes = self.client.files.download_bytes(id=function.file_id)
        except CogniteAPIError as e:
            if e.code == 400 and "File ids not found" in e.message:
                HighSeverityWarning(
                    f"The function {function.external_id!r} does not have code to dump. It is not available in CDF."
                ).print_warning()
                return
            raise
        try:
            top_level = f"{to_directory_compatible(function.external_id or 'unknown_external_id')}/"
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                if all(name.startswith(top_level) for name in zf.namelist()):
                    zf.extractall(folder)
                else:
                    zf.extractall(folder / top_level)
        except zipfile.BadZipFile as e:
            HighSeverityWarning(
                f"The function {function.external_id!r} has a corrupted code zip file. Unable to extract code: {e!s}"
            ).print_warning()


class StreamlitFinder(ResourceFinder[tuple[str, ...]]):
    def __init__(self, client: ToolkitClient, identifier: tuple[str, ...] | None = None):
        super().__init__(client, identifier)
        self.apps: StreamlitList | None = None

    def _interactive_select(self) -> tuple[str, ...]:
        """Interactively select one or more Streamlit apps to dump."""
        result = self.client.documents.aggregate_unique_values(
            SourceFileProperty.metadata_key("creator"),
            filter=filters.Equals(SourceFileProperty.directory, "/streamlit-apps/"),
        )
        if not result:
            raise ToolkitMissingResourceError("No Streamlit apps found")

        selected_creator = questionary.select(
            "Who is the creator of the Streamlit app you would like to dump? [name (app count)]",
            choices=[
                Choice(f"{item.value} ({item.count})", value=item.value)
                for item in sorted(result, key=lambda r: (r.count, str(r.value) or ""))
            ],
        ).ask()
        files = self.client.files.list(
            limit=-1, directory_prefix="/streamlit-apps/", metadata={"creator": str(selected_creator)}
        )
        self.apps = StreamlitList([Streamlit.from_file(file) for file in files if file.name and file.external_id])
        if missing := [file for file in files if not file.external_id or file.name]:
            MediumSeverityWarning(
                f"{len(missing)} file(s) in /streamlit-apps/ are missing "
                f"either name or external ID and will be skipped. File IDs: {humanize_collection([file.id for file in missing])}",
            ).print_warning()
        selected_ids: list[str] | None = questionary.checkbox(
            message="Which Streamlit app(s) would you like to dump?",
            choices=[
                Choice(
                    title=f"{app.name} ({app.creator} - {ms_to_datetime(app.last_updated_time)})", value=app.external_id
                )
                for app in sorted(self.apps, key=lambda a: a.name)
            ],
        ).ask()
        if not selected_ids:
            raise ToolkitValueError("No Streamlit app selected for dumping. Aborting...")
        return tuple(selected_ids)

    def __iter__(self) -> Iterator[tuple[list[Hashable], CogniteResourceList | None, ResourceCRUD, None | str]]:
        identifier = self.identifier or self._interactive_select()
        loader = StreamlitCRUD.create_loader(self.client)
        # If the user used interactive select, we have already downloaded the streamlit apps,
        # Thus, we do not need to download them again. If not pass the identifier and let the main logic
        # take care of the download.
        if self.apps:
            yield [], StreamlitList([app for app in self.apps if app.external_id in identifier]), loader, None
        else:
            yield list(identifier), None, loader, None

    def dump_code(self, app: Streamlit, folder: Path, console: Console | None = None) -> None:
        """Dump the code of a Streamlit app to the specified folder.

        The code is extracted from the JSON content of the app file in CDF.

        Args:
            app (Streamlit): The Streamlit app whose code is to be dumped.
            folder (Path): The directory where the app code will be saved.
            console (Console | None): Optional Rich console for printing warnings.
        """
        try:
            content = self.client.files.download_bytes(external_id=app.external_id)
        except CogniteAPIError as e:
            if e.code == 400 and e.missing:
                HighSeverityWarning(
                    f"The source code for {app.external_id!r} could not be retrieved from CDF."
                ).print_warning(console=console)
                return
            raise

        try:
            json_content = json.loads(content)
        except json.JSONDecodeError as e:
            HighSeverityWarning(
                f"The JSON content for the Streamlit app {app.external_id!r} is corrupt and could not be extracted. "
                f"Download file with the same external id manually to remediate. {e!s}"
            ).print_warning(console=console)
            return

        app_folder = to_directory_compatible(app.external_id)
        app_path = folder / app_folder
        app_path.mkdir(exist_ok=True)
        if isinstance(json_content.get("requirements"), list):
            requirements_txt = app_path / "requirements.txt"
            requirements = json_content["requirements"]
            if not requirements:
                HighSeverityWarning(
                    f"The Streamlit app {app.external_id!r} has a requirements.txt file with no content. Skipping..."
                ).print_warning()
            else:
                requirements_txt.write_text("\n".join(requirements), encoding="utf-8")
        files = json_content.get("files", {})
        if not isinstance(files, dict) or (not files):
            HighSeverityWarning(
                f"The Streamlit app {app.external_id!r} does not have any files to dump. It is likely corrupted."
            ).print_warning(console=console)
            return
        created_files: set[str] = set()
        for relative_filepath, content in files.items():
            filepath = app_path / relative_filepath
            filepath.parent.mkdir(parents=True, exist_ok=True)
            file_content = content.get("content", {}).get("text", "")
            if not isinstance(file_content, str):
                HighSeverityWarning(
                    f"The Streamlit app {app.external_id!r} has a file {relative_filepath} with invalid content. Skipping..."
                ).print_warning(console=console)
                continue
            safe_write(filepath, file_content, encoding="utf-8")
            created_files.add(relative_filepath)

        entry_point = json_content.get("entrypoint")
        if entry_point and entry_point not in created_files:
            HighSeverityWarning(
                f"The Streamlit app {app.external_id!r} has an entry point {entry_point} that was not found in the files. "
                "The app may be corrupted."
            ).print_warning(console=console)


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

        dumped_ids: list[Hashable] = []
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
                resource_id = loader.get_id(resource)
                name = loader.as_str(resource_id)
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
                if isinstance(finder, FunctionFinder) and isinstance(resource, Function):
                    finder.dump_function_code(resource, resource_folder)
                if isinstance(finder, StreamlitFinder) and isinstance(resource, Streamlit):
                    finder.dump_code(resource, resource_folder)
                dumped_ids.append(resource_id)
        print(Panel(f"Dumped {humanize_collection(dumped_ids)}", title="Success", style="green", expand=False))
