import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Generic, cast

from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import UniqueResult
from cognite.client.data_classes.assets import AssetProperty
from cognite.client.data_classes.documents import SourceFileProperty
from cognite.client.data_classes.events import EventProperty
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier, T_RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APM_CONFIG_SPACE,
    APMConfigResponse,
    Discipline,
    FeatureConfiguration,
    RootLocationConfiguration,
    RootLocationFeatureToggles,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeReference,
    NodeRequest,
    SpaceRequest,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    INFIELD_ON_CDM_DATA_MODEL,
    InFieldCDMLocationConfigRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import TypedNodeIdentifier
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import LocationFilterRequest
from cognite_toolkit._cdf_tk.cruds import (
    InFieldCDMLocationConfigCRUD,
    LocationFilterCRUD,
    NodeCRUD,
    ResourceCRUD,
    SpaceCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMigrationError,
    ToolkitMissingResourceError,
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name, warn_invalid_space_name

from .data_model import CREATED_SOURCE_SYSTEM_VIEW_ID, SPACE, SPACE_SOURCE_VIEW_ID


@dataclass
class CreatedResource(Generic[T_RequestResource]):
    resource: T_RequestResource
    config_data: dict[str, Any] | None = None
    filestem: str | None = None


@dataclass
class ToCreateResources(Generic[T_Identifier, T_RequestResource, T_ResponseResource]):
    resources: Sequence[CreatedResource[T_RequestResource]]
    crud_cls: type[ResourceCRUD[T_Identifier, T_RequestResource, T_ResponseResource]]
    display_name: str
    store_linage: Callable[[], int] | None = None


class MigrationCreator(ABC):
    """Base class for migration resources configurations that are created resources."""

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def create_resources(self) -> Iterable[ToCreateResources]:
        raise NotImplementedError("Subclasses should implement this method")


class InstanceSpaceCreator(MigrationCreator):
    """Creates instance spaces for migration."""

    def __init__(
        self,
        client: ToolkitClient,
        datasets: list[DataSetResponse] | None = None,
        data_set_external_ids: list[str] | None = None,
        auto_fix: bool = False,
    ) -> None:
        super().__init__(client)
        if sum([datasets is not None, data_set_external_ids is not None]) != 1:
            raise ValueError("Exactly one of datasets or data_set_external_ids must be provided.")
        self.data_set_external_ids = data_set_external_ids
        self.datasets = datasets or []
        self.auto_fix = auto_fix

    def create_resources(self) -> Iterable[ToCreateResources]:
        if self.data_set_external_ids is not None:
            self.datasets = self.client.tool.datasets.retrieve(ExternalId.from_external_ids(self.data_set_external_ids))

        if missing_external_ids := [ds.id for ds in self.datasets if ds.external_id is None]:
            raise ToolkitRequiredValueError(
                f"Cannot create instance spaces for datasets with missing external IDs: {humanize_collection(missing_external_ids)}"
            )

        space_id_by_dataset = self._resolve_space_ids()

        created_resources: list[CreatedResource[SpaceRequest]] = []
        linage_nodes: list[NodeRequest] = []
        for dataset in self.datasets:
            data_set_external_id = cast(str, dataset.external_id)
            space_id = space_id_by_dataset[data_set_external_id]
            space = SpaceRequest(
                space=space_id,
                name=dataset.name,
                description=dataset.description,
            )
            linage_node = NodeRequest(
                space=SPACE.space,
                external_id=space.space,
                sources=[
                    InstanceSource(
                        source=SPACE_SOURCE_VIEW_ID,
                        properties={
                            "instanceSpace": space.space,
                            "dataSetId": dataset.id,
                            "dataSetExternalId": data_set_external_id,
                        },
                    )
                ],
            )
            linage_nodes.append(linage_node)
            created_resources.append(
                CreatedResource(
                    resource=space,
                    config_data=space.dump(),
                    filestem=space.space,
                )
            )
        yield ToCreateResources(
            resources=created_resources,
            crud_cls=SpaceCRUD,
            display_name="Instance Space",
            store_linage=lambda: self.store_lineage(linage_nodes),
        )

    def _resolve_space_ids(self) -> dict[str, str]:
        """Compute the space ID for each dataset external ID, applying auto-fix if enabled.

        Raises ToolkitMigrationError if multiple dataset external IDs resolve to the same space ID.
        """
        space_id_to_external_ids: dict[str, list[str]] = {}
        result: dict[str, str] = {}

        for dataset in self.datasets:
            data_set_external_id = cast(str, dataset.external_id)
            warning_message = warn_invalid_space_name(data_set_external_id)
            space_id = data_set_external_id
            if warning_message and self.auto_fix:
                space_id = fix_invalid_space_name(data_set_external_id)
                LowSeverityWarning(
                    f"{warning_message}\nAutomatically fixing space name to {space_id!r}."
                ).print_warning(console=self.client.console)
            elif warning_message:
                HighSeverityWarning(
                    f"{warning_message}\nRun command with `--auto-fix` to automatically make the data set external ID a valid space ID."
                ).print_warning(console=self.client.console)

            result[data_set_external_id] = space_id
            space_id_to_external_ids.setdefault(space_id, []).append(data_set_external_id)

        duplicates = {
            space_id: external_ids
            for space_id, external_ids in space_id_to_external_ids.items()
            if len(external_ids) > 1
        }
        if duplicates:
            parts = [
                f"  space {space_id!r} <- datasets {humanize_collection(ext_ids)}"
                for space_id, ext_ids in duplicates.items()
            ]
            raise ToolkitMigrationError(
                "Multiple dataset external IDs resolve to the same space ID:\n" + "\n".join(parts)
            )

        return result

    def store_lineage(self, lineage_nodes: Sequence[NodeRequest]) -> int:
        res = self.client.tool.instances.create(lineage_nodes)
        return len(res)


class SourceSystemCreator(MigrationCreator):
    COGNITE_SOURCE_SYSTEM_VIEW_ID = ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1")

    def __init__(
        self,
        client: ToolkitClient,
        instance_space: str,
        data_set_external_id: str | None = None,
        hierarchy: str | None = None,
    ) -> None:
        super().__init__(client)
        if sum([data_set_external_id is not None, hierarchy is not None]) != 1:
            raise ValueError("Exactly one of data_set_external_id or hierarchy must be provided.")
        self._instance_space = instance_space
        self.data_set_external_id = data_set_external_id
        self.hierarchy = hierarchy

    def create_resources(self) -> Iterable[ToCreateResources]:
        existing_resources = self._get_existing_source_systems()
        seen: set[str] = set()
        to_create: list[CreatedResource[NodeRequest]] = []
        for source in self._lookup_sources():
            source_str = source.value
            if not isinstance(source_str, str) or source_str in seen:
                continue
            seen.add(source_str)
            if existing_id := existing_resources.get(source_str):
                self.client.console.print(f"Skipping {source_str} as it already exists in {existing_id!r}.")
                continue
            data_source = InstanceSource(source=self.COGNITE_SOURCE_SYSTEM_VIEW_ID, properties={"name": source_str})
            linage_source = InstanceSource(source=CREATED_SOURCE_SYSTEM_VIEW_ID, properties={"source": source_str})
            to_create.append(
                CreatedResource[NodeRequest](
                    resource=NodeRequest(
                        space=self._instance_space,
                        external_id=source_str,
                        sources=[data_source, linage_source],
                    ),
                    config_data=NodeRequest(
                        space=self._instance_space,
                        external_id=source_str,
                        sources=[data_source],
                    ).dump(),
                    filestem=source_str,
                )
            )
        yield ToCreateResources(
            resources=to_create,
            crud_cls=NodeCRUD,
            display_name="Source System",
            store_linage=lambda: len(to_create),
        )

    def _get_existing_source_systems(self) -> dict[str, NodeReference]:
        all_existing = self.client.migration.created_source_system.list(limit=-1)
        return {node.source: NodeReference(space=node.space, external_id=node.external_id) for node in all_existing}

    def _lookup_sources(self) -> Iterable[UniqueResult]:
        yield from self.client.assets.aggregate_unique_values(AssetProperty.source, filter=self._simple_filter)
        yield from self.client.events.aggregate_unique_values(property=EventProperty.source, filter=self._simple_filter)
        yield from self.client.documents.aggregate_unique_values(
            SourceFileProperty.source, filter=self._advanced_filter, limit=1000
        )

    @cached_property
    def _simple_filter(self) -> dict[str, Any] | None:
        if self.data_set_external_id is not None:
            return {"dataSetIds": [{"externalId": self.data_set_external_id}]}
        if self.hierarchy is not None:
            return {"assetSubtreeIds": [{"externalId": self.hierarchy}]}
        return None

    @cached_property
    def _advanced_filter(self) -> filters.Filter:
        if self.data_set_external_id is not None:
            data_set_id = self.client.lookup.data_sets.id(self.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(f"Data set with external ID '{self.data_set_external_id}' not found.")
            return filters.Equals(SourceFileProperty.data_set_id, data_set_id)
        if self.hierarchy is not None:
            return filters.InAssetSubtree(SourceFileProperty.asset_external_ids, [self.hierarchy])
        else:
            raise ValueError("This should not happen.")


class InfieldV2ConfigCreator(MigrationCreator):
    TARGET_SPACE = "APM_Config"

    def __init__(
        self,
        client: ToolkitClient,
        external_ids: Sequence[str] | None = None,
        apm_configs: Sequence[APMConfigResponse] | None = None,
    ) -> None:
        super().__init__(client)
        if sum([external_ids is not None, apm_configs is not None]) != 1:
            raise ValueError("Exactly one of external_ids or apm_configs must be provided.")
        self.external_ids = external_ids
        self.apm_configs = apm_configs

    def create_resources(self) -> Iterable[ToCreateResources]:
        if self.external_ids is not None:
            apm_configs = self.client.infield.apm_config.retrieve(
                TypedNodeIdentifier.from_str_ids(self.external_ids, space=APM_CONFIG_SPACE)
            )
        else:
            # We know this is not None from the check in __init__
            apm_configs = list(cast(Sequence[APMConfigResponse], self.apm_configs))

        all_location_configs: list[CreatedResource[InFieldCDMLocationConfigRequest]] = []
        all_location_filters: list[CreatedResource[LocationFilterRequest]] = []
        for apm_config in apm_configs:
            location_configs, location_filters = self._create_infield_v2_config(apm_config)
            all_location_configs.extend(
                CreatedResource(
                    resource=loc_config,
                    config_data=loc_config.dump(context="toolkit", exclude={"instance_type"}),
                    filestem=f"{apm_config.external_id}_location_{loc_config.name}",
                )
                for loc_config in location_configs
            )
            all_location_filters.extend(
                CreatedResource(
                    resource=loc_filter,
                    config_data=loc_filter.dump(),
                    filestem=f"{apm_config.external_id}_filter_{loc_filter.name}",
                )
                for loc_filter in location_filters
            )
        yield ToCreateResources(
            resources=all_location_filters,
            crud_cls=LocationFilterCRUD,
            display_name="Location Filters",
        )
        yield ToCreateResources(
            resources=all_location_configs,
            crud_cls=InFieldCDMLocationConfigCRUD,
            display_name="InField CDM Location Configs",
        )

    def _create_infield_v2_config(
        self, config: APMConfigResponse
    ) -> tuple[list[InFieldCDMLocationConfigRequest], list[LocationFilterRequest]]:
        location_configs: list[InFieldCDMLocationConfigRequest] = []
        location_filters: list[LocationFilterRequest] = []
        if not config.feature_configuration:
            return location_configs, location_filters

        data_exploration = self._create_data_exploration(config.feature_configuration)

        for index, root_location_config in enumerate(config.feature_configuration.root_location_configurations or []):
            location_filter = self._create_location_filter(root_location_config)
            location_filters.append(location_filter)
            location_configs.append(
                self._create_location_config(
                    root_location_config,
                    config.feature_configuration.disciplines,
                    data_exploration,
                    index,
                )
            )

        return location_configs, location_filters

    def _create_location_filter(self, config: RootLocationConfiguration) -> LocationFilterRequest:
        original_external_id = config.external_id or config.asset_external_id or str(uuid.uuid4())
        external_id = f"location_filter_{original_external_id}"
        name = config.display_name or config.asset_external_id or external_id

        instance_spaces = [
            space
            for space in [config.app_data_instance_space, config.source_data_instance_space]
            if space is not None and space != ""
        ]

        # Todo: Scene and views
        return LocationFilterRequest(
            external_id=external_id,
            name=name,
            description="InField location, migrated from old location configuration",
            instance_spaces=instance_spaces or None,
            data_models=[INFIELD_ON_CDM_DATA_MODEL],
            data_modeling_type="DATA_MODELING_ONLY",
        )

    def _create_location_config(
        self,
        config: RootLocationConfiguration,
        disciplines: list[Discipline] | None,
        data_exploration: dict[str, JsonValue],
        index: int,
    ) -> InFieldCDMLocationConfigRequest:
        if (
            config.asset_external_id is None
            or config.app_data_instance_space is None
            or config.source_data_instance_space is None
        ):
            raise ToolkitMigrationError(
                f"Root location configuration with external ID '{config.external_id}' is missing required fields for migration. "
            )
        root_node = self.client.migration.lookup.assets(external_id=config.asset_external_id)
        if not root_node:
            raise ToolkitMissingResourceError(
                f"Root asset with external ID '{config.asset_external_id}' not migrated. Cannot create location config for root location configuration with external ID '{config.external_id}'."
            )

        if config.external_id:
            external_id = config.external_id
        else:
            external_id = f"{config.asset_external_id}_{index}"

        feature_toggles: dict[str, Any] | None = None
        if isinstance(config.feature_toggles, RootLocationFeatureToggles):
            feature_toggles = config.feature_toggles.dump()
            if config.feature_toggles.observations:
                feature_toggles["observations"] = config.feature_toggles.observations.is_enabled

        access_management: dict[str, JsonValue] = {}
        if config.template_admins:
            # Todo Fetch and update groups. Jira ticket: CDF-27033
            # list[str] is a valid JsonValue
            access_management["templateAdmins"] = config.template_admins  # type: ignore[assignment]
        if config.checklist_admins:
            access_management["checklistAdmins"] = config.checklist_admins  # type: ignore[assignment]

        data_filters: dict[str, JsonValue] = {}
        for key in ["assets", "files", "timeseries", "maintenanceOrders", "operations", "notifications"]:
            # Todo Assets does not support multiple instanceSpaces.
            #    add to Validation in cdf build.
            data_filters[key] = {"instanceSpaces": [config.source_data_instance_space]}

        data_storage: dict[str, JsonValue] = {
            "rootLocation": root_node.dump(include_instance_type=False),
            "appInstanceSpace": config.app_data_instance_space,
        }
        view_mappings: dict[str, JsonValue] = {}
        for key, default_view in [
            ("asset", ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1")),
            ("operation", ViewReference(space="cdf_idm", external_id="CogniteOperation", version="v1")),
            ("notification", ViewReference(space="cdf_idm", external_id="CogniteNotification", version="v1")),
            ("maintenanceOrder", ViewReference(space="cdf_idm", external_id="CogniteMaintenanceOrder", version="v1")),
            ("file", ViewReference(space="cdf_cdm", external_id="CogniteFile", version="v1")),
        ]:
            view_mappings[key] = default_view.dump()

        return InFieldCDMLocationConfigRequest(
            space=config.source_data_instance_space or "<Please fill in the source data instance space>",
            external_id=external_id,
            name="InField Location Config",
            description="Migrated InField Location Configuration",
            feature_toggles=feature_toggles,
            access_management=access_management or None,
            data_filters=data_filters,
            disciplines=[discipline.dump() for discipline in disciplines] if disciplines else None,
            data_storage=data_storage,
            view_mappings=view_mappings,
            data_exploration_config=data_exploration or None,
        )

    def _create_data_exploration(self, config: FeatureConfiguration) -> dict[str, JsonValue]:
        data_exploration: dict[str, JsonValue] = {}
        if config.observations:
            data_exploration["observations"] = config.observations
        if config.documents:
            documents: dict[str, JsonValue] = {}
            if config.documents.type:
                documents["type"] = config.documents.type.removeprefix("metadata.")
            if config.documents.title:
                documents["title"] = config.documents.title
            if config.documents.description:
                documents["description"] = config.documents.description.removeprefix("metadata.")
            data_exploration["documents"] = documents
        if config.notifications:
            data_exploration["notifications"] = config.notifications.dump()
        if config.asset_page_configuration:
            dumped = config.asset_page_configuration.dump()
            # Linkable Asset Key is used for implicit connections in the old Asset.metaadata.
            dumped.pop("linkableAssetKeys", None)
            data_exploration["assets"] = dumped
        return data_exploration
