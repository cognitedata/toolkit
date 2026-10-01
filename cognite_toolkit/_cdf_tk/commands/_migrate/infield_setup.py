from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Literal

import questionary
import typer
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeOrEdgeRequest, NodeOrEdgeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import (
    ENTITY_BY_SOURCE_VIEW_EXTERNAL_ID,
    SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID,
    create_apm_source_data_mappings,
    get_first_instance_space,
    resolve_apm_source_data_instance_spaces,
    resolve_source_data_view_ids,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    APMSourceDataMaintenanceOrderMapping,
    ConnectionCreator,
    CustomConnectionMapping,
    InFieldAssetMapping,
    InFieldConditionMapping,
    InFieldObservationSapStatusMapping,
    InFieldUserMapping,
    InstanceIdMapper,
    LocationSplitInstanceIdMapper,
    SpaceMappingInstanceIdMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
    DataMapper,
    FDMtoCDMMapper,
    InFieldLegacyToCDMScheduleMapper,
    LocationSplitFDMtoCDMMapper,
    LocationSplitSolutionTagMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    DIRECT_RELATION_EDGE_TIEBREAKERS,
    create_infield_data_mappings,
    create_infield_schedule_selector,
    resolve_observation_view_id,
)
from cognite_toolkit._cdf_tk.commands._migrate.location_split import (
    COGNITE_SOLUTION_TAG_VIEW_ID,
    build_target_by_root_asset,
    find_shared_legacy_instance_spaces,
)
from cognite_toolkit._cdf_tk.dataio.selectors import (
    InstanceSelector,
    InstanceViewSelector,
    SelectedView,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection


@dataclass
class InfieldMigrationSpaces:
    source: str
    _target: str | None

    @property
    def target(self) -> str:
        if self._target is None:
            raise RuntimeError("Bug in Toolkit. Target space is None, cannot access target_space property.")
        return self._target

    @property
    def is_location_split(self) -> bool:
        return self._target is None


class InFieldLookup:
    """Utility class that caches responses from the CDF client to avoid repeated API calls
    during setting up the InField data migration."""

    def __init__(self, client: ToolkitClient, operation: Literal["Infield data", "APM_SourceData"]) -> None:
        self.client = client
        self.operation = operation
        self._source_space: str | None = None

    @property
    def source_space(self) -> str:
        if self._source_space is None:
            raise ValueError("Source space has not been set.")
        return self._source_space

    @source_space.setter
    def source_space(self, value: str) -> None:
        if self._source_space is not None:
            raise ValueError("Source space has already been set and cannot be changed.")
        self._source_space = value

    @cached_property
    def apm_configs(self) -> Sequence[APMConfigResponse]:
        return self.client.infield.apm_config.list(limit=None)

    @cached_property
    def cdm_configs(self) -> Sequence[InFieldCDMLocationConfigResponse]:
        return self.client.infield.cdm_config.list(limit=None)

    @cached_property
    def target_by_root_asset(self) -> dict[str, str]:
        return build_target_by_root_asset(
            self.client,
            source_space=self.source_space,
            apm_configs=self.apm_configs,
            cdm_configs=self.cdm_configs,
            target_kind="app_data" if self.operation == "Infield data" else "source_data",
        )

    @cached_property
    def target_spaces(self) -> set[str]:
        return set(self.target_by_root_asset.values())

    @cached_property
    def shared_legacy_spaces(self) -> set[str]:
        return find_shared_legacy_instance_spaces(self.apm_configs)


class InFieldUserInput:
    """Handles user input for InField migration, including prompting for source and target spaces,"""

    def __init__(self, client: ToolkitClient, lookup: InFieldLookup) -> None:
        self.client = client
        self.lookup = lookup
        self.operation = lookup.operation

    @staticmethod
    def prompt_flags(log_dir: Path, dry_run: bool, verbose: bool) -> tuple[Path, bool, bool]:
        log_dir = Path(
            questionary.path("Specify log directory for migration logs:", default=log_dir.as_posix()).unsafe_ask()
        )
        dry_run = questionary.confirm("Do you want to perform a dry run?", default=dry_run).unsafe_ask()
        verbose = questionary.confirm("Do you want verbose output?", default=verbose).unsafe_ask()
        return log_dir, dry_run, verbose

    def prompt_migration_spaces(self) -> InfieldMigrationSpaces:
        source_candidates = self._source_candidates
        source_space = self._prompt_space(source_candidates, "source")
        if self._is_split_location(source_space):
            return InfieldMigrationSpaces(source=source_space, _target=None)

        target_space = self._prompt_space(self._target_candidates, "target")
        return InfieldMigrationSpaces(source=source_space, _target=target_space)

    def validate_migration_spaces(
        self, user_source_space: str, user_target_space: str | None
    ) -> InfieldMigrationSpaces:
        self._is_valid_space(user_source_space, self._source_candidates, "source")
        if self._is_split_location(user_source_space):
            if user_target_space is not None:
                raise typer.BadParameter(
                    f"Source space {user_source_space!r} is shared by multiple InField locations; These must be split into "
                    "multiple target spaces during migration. You should rerun this command without --target-space so "
                    "Toolkit can determine the appropriate target spaces from the deployed location configs."
                )
            if not self._target_candidates:
                raise typer.BadParameter(
                    "No InfieldOnCDM Configurations with app instance space found. Cannot migrate Infield data. "
                    "Have you placed the CDMLocationConfig YAMLs generated by 'cdf migrate infield-configs' "
                    "into a Toolkit module and deployed them with 'cdf deploy'?"
                )
            return InfieldMigrationSpaces(source=user_source_space, _target=None)
        if user_target_space is None:
            raise typer.BadParameter(
                f"Target space must be provided for non-split {self.operation} migration. "
                f"Available target spaces are: {humanize_collection(self._target_candidates)}."
            )
        self._is_valid_space(user_target_space, self._target_candidates, "target")

        return InfieldMigrationSpaces(source=user_source_space, _target=user_target_space)

    def _prompt_space(self, candidates: set[str], space_label: Literal["source", "target"]) -> str:
        existing_candidates = self._get_space_stats(candidates)
        if missing := candidates - existing_candidates.keys():
            HighSeverityWarning(
                f"The following {space_label} spaces do not exist or cannot be accessed: {humanize_collection(missing)}."
            ).print_warning(console=self.client.console)
        stats = [existing_candidates[space] for space in candidates if space in existing_candidates]
        if not stats:
            raise typer.BadParameter(
                f"No {space_label} spaces exist or can be accessed for {self.operation} migration."
            )
        selected_space = questionary.select(
            f"Select the {space_label} instance space for {self.operation} migration:",
            choices=[
                questionary.Choice(
                    title=f"{item.space} (contains {item.nodes:,} nodes and {item.edges:,} edges)",
                    value=item.space,
                )
                for item in stats
            ],
        ).unsafe_ask()
        if not isinstance(selected_space, str):
            raise typer.BadParameter(f"No {space_label} space selected for {self.operation} migration.")
        return selected_space

    def _is_valid_space(self, user_space: str, candidates: set[str], space_label: Literal["source", "target"]) -> None:
        """Checks if the user-provided space is valid and exists in the candidates. Raises a BadParameter exception if not."""
        if not candidates:
            raise typer.BadParameter(
                f"No {space_label} spaces are available for {self.operation} migration. "
                "Please ensure the CDF instance has the appropriate InField configurations deployed."
            )
        if user_space not in candidates:
            raise typer.BadParameter(
                f"{space_label.capitalize()} space '{user_space}' is not a valid {space_label} for {self.operation} migration. "
                f"Available {space_label} spaces are: {humanize_collection(candidates)}."
            )
        if not self._get_space_stats({user_space}):
            raise typer.BadParameter(
                f"{space_label.capitalize()} space '{user_space}' does not exist or cannot be accessed. "
                f"Please ensure the {self.operation} instance space contains data and can be accessed."
            )

    def _is_split_location(self, source_space: str) -> bool:
        return Flags.INFIELD_LOCATION_SPLIT.is_enabled() and source_space in self.lookup.shared_legacy_spaces

    @cached_property
    def _source_candidates(self) -> set[str]:
        if self.operation == "Infield data":
            return {
                location.app_data_instance_space
                for config in self.lookup.apm_configs
                if config.feature_configuration
                for location in config.feature_configuration.root_location_configurations or []
                if location.app_data_instance_space is not None
            }
        else:
            return resolve_apm_source_data_instance_spaces(self.lookup.apm_configs)

    @property
    def _target_candidates(self) -> set[str]:
        if self.operation == "Infield data":
            return {
                config.data_storage.app_instance_space
                for config in self.lookup.cdm_configs
                if config.data_storage and config.data_storage.app_instance_space
            }
        else:
            return {
                space
                for config in self.lookup.cdm_configs
                for type_key in SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID.values()
                if (space := get_first_instance_space(config.data_filters, type_key)) is not None
            }

    def _get_space_stats(self, spaces: set[str]) -> dict[str, SpaceStatistics]:
        return {stat.space: stat for stat in self.client.data_modeling.statistics.spaces.retrieve(list(spaces))}


class InFieldSetup:
    """Handles the setup of InField data migration, including creating instance ID mappers and data mappers."""

    def __init__(self, client: ToolkitClient, lookup: InFieldLookup) -> None:
        self.client = client
        self.lookup = lookup

    def _create_instance_id_mappers(
        self, migration_spaces: InfieldMigrationSpaces, passthrough: dict[str, str]
    ) -> InstanceIdMapper:
        if not migration_spaces.is_location_split:
            return SpaceMappingInstanceIdMapper({migration_spaces.source: migration_spaces.target, **passthrough})

        return LocationSplitInstanceIdMapper(
            self.client,
            migration_spaces.source,
            passthrough_space_mapping=passthrough or None,
            target_spaces=self.lookup.target_spaces,
        )

    def infield_mappings(
        self, migration_spaces: InfieldMigrationSpaces, skip_observations: bool = False
    ) -> list[ViewToViewMapping]:
        mappings = create_infield_data_mappings()
        if skip_observations:
            # Skip the default mapping to the FieldObservation view if users will be using custom observation views.
            # If this skip is not done, users will end up with observations both in the custom observation view and the default FieldObservation view,
            # which can lead to the wrong view being rendered for migrated observations in Infield since it relies on the instances/inspect endpoint.
            return [mapping for mapping in mappings if mapping.destination_view.external_id != "FieldObservation"]

        # If a custom observation view is configured for the target space (e.g. to support SAP writeback),
        # migrate Observations onto it instead of the default FieldObservation view.
        target_spaces = self._get_target_spaces(migration_spaces)
        custom_observation_views = {
            resolve_observation_view_id(self.lookup.cdm_configs, space) for space in target_spaces
        }
        if len(custom_observation_views) > 1:
            raise ToolkitMigrationError(
                "Location split targets disagree on the custom observation view. "
                f"Distinct views: {humanize_collection([str(view_id) for view_id in custom_observation_views])}."
            )
        custom_observation_view = next(iter(custom_observation_views), None)
        if custom_observation_view is not None:
            mappings = [
                m.model_copy(update={"destination_view": custom_observation_view})
                if m.source_view.external_id == "Observation"
                else m
                for m in mappings
            ]
        return mappings

    @classmethod
    def get_infield_data_selectors(
        cls, migration_spaces: InfieldMigrationSpaces, infield_mappings: list[ViewToViewMapping]
    ) -> list[InstanceSelector]:
        """Creates instance selectors for InField data migration based on the provided mappings and migration spaces."""
        selectors: list[InstanceSelector] = []
        for mapping in infield_mappings:
            if mapping.source_view.external_id == "Schedule":
                # Special case for schedules, see create_infield_schedule_query for docs on why.
                selectors.append(create_infield_schedule_selector(instance_space=migration_spaces.source))
                continue

            edge_types = list(mapping.edge_mapping.keys()) if mapping.edge_mapping else []
            selectors.append(
                InstanceViewSelector(
                    view=SelectedView(
                        space=mapping.source_view.space,
                        external_id=mapping.source_view.external_id,
                        version=mapping.source_view.version,
                    ),
                    instance_spaces=(migration_spaces.source,),
                    edge_types=tuple(dict.fromkeys(edge_types)) or None,
                    endpoint="sync",
                )
            )
        return selectors

    def get_infield_data_mapper(
        self, migration_spaces: InfieldMigrationSpaces, infield_mappings: list[ViewToViewMapping]
    ) -> FDMtoCDMMapper:
        """Creates a data mapper for InField data migration based on the provided mappings and migration spaces."""
        instance_id_mapper = self._create_instance_id_mappers(
            migration_spaces, passthrough={"cognite_app_data": "cognite_app_data"}
        )
        connection_creator = ConnectionCreator(
            self.client,
            instance_id_mapper=instance_id_mapper,
            custom_mappings=[InFieldAssetMapping(self.client)],
            direct_relation_edge_tiebreakers=DIRECT_RELATION_EDGE_TIEBREAKERS,
        )
        custom_properties_mappings = [
            InFieldConditionMapping(infield_mappings),
            InFieldUserMapping(),
            InFieldObservationSapStatusMapping(),
        ]
        location_split_id_mapper = (
            instance_id_mapper if isinstance(instance_id_mapper, LocationSplitInstanceIdMapper) else None
        )
        schedule_mapper = self._create_schedule_mapper(connection_creator, infield_mappings, location_split_id_mapper)

        custom_instance_mappings: dict[ViewId, DataMapper[InstanceSelector, NodeOrEdgeResponse, NodeOrEdgeRequest]] = {
            schedule_mapper.SCHEDULE_VIEW: schedule_mapper
        }

        if not migration_spaces.is_location_split:
            return FDMtoCDMMapper(
                self.client,
                infield_mappings,
                connection_creator=connection_creator,
                custom_properties_mappings=custom_properties_mappings,
                custom_instance_mappings=custom_instance_mappings,
            )

        if not location_split_id_mapper:
            raise RuntimeError(
                "Toolkit bug: LocationSplitInstanceIdMapper should be used for location split migrations."
            )

        custom_instance_mappings[COGNITE_SOLUTION_TAG_VIEW_ID] = self._create_solution_tag_mapper(
            connection_creator, infield_mappings
        )

        return LocationSplitFDMtoCDMMapper(
            self.client,
            infield_mappings,
            connection_creator,
            location_split_id_mapper,
            self.lookup.target_by_root_asset,
            custom_properties_mappings=custom_properties_mappings,
            custom_instance_mappings=custom_instance_mappings,
        )

    def _create_schedule_mapper(
        self,
        connection_creator: ConnectionCreator,
        infield_mappings: list[ViewToViewMapping],
        location_split_id_mapper: LocationSplitInstanceIdMapper | None,
    ) -> InFieldLegacyToCDMScheduleMapper:
        schedule_mapping = next((m for m in infield_mappings if m.source_view.external_id == "Schedule"), None)
        if schedule_mapping is None:
            raise RuntimeError("Toolkit bug: Schedule mapping should always be present in InField data mappings.")
        schedule_mapper = InFieldLegacyToCDMScheduleMapper(
            self.client, connection_creator, schedule_mapping, location_split_id_mapper
        )
        return schedule_mapper

    def _create_solution_tag_mapper(
        self, connection_creator: ConnectionCreator, infield_mappings: list[ViewToViewMapping]
    ) -> LocationSplitSolutionTagMapper:
        solution_tag_mapping = next(
            (m for m in infield_mappings if m.source_view == COGNITE_SOLUTION_TAG_VIEW_ID), None
        )
        if solution_tag_mapping is None:
            raise RuntimeError("Toolkit bug: SolutionTag mapping should always be present in InField data mappings.")
        solution_tag_mapper = LocationSplitSolutionTagMapper(
            self.client, connection_creator, solution_tag_mapping, self.lookup.target_spaces
        )
        return solution_tag_mapper

    def create_source_mappings(
        self, migration_spaces: InfieldMigrationSpaces, source_views: dict[str, ViewId]
    ) -> list[ViewToViewMapping]:
        mappings = create_apm_source_data_mappings()
        target_spaces = self._get_target_spaces(migration_spaces)

        custom_views: dict[str, ViewId | None] = {}
        for space in target_spaces:
            space_custom_views, custom_view_warnings = resolve_source_data_view_ids(self.lookup.cdm_configs, space)
            for warning in custom_view_warnings:
                self.client.console.print(
                    Panel(
                        warning,
                        title="Conflicting custom view configuration detected",
                        expand=False,
                        border_style="yellow",
                    )
                )
            for type_key in SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID.values():
                view_id = space_custom_views.get(type_key)
                if type_key in custom_views and custom_views[type_key] != view_id:
                    raise ToolkitMigrationError(
                        f"Target locations disagree on the custom {type_key} view: {custom_views[type_key]!s} vs {view_id!s}."
                    )
                custom_views[type_key] = view_id
        custom_views = {type_key: view_id for type_key, view_id in custom_views.items() if view_id is not None}
        if custom_views:
            # Custom maintenanceOrder/operation/notification views, keyed off the original APM source view IDs.
            remapped: list[ViewToViewMapping] = []
            for mapping in mappings:
                source_type_key = SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID.get(mapping.source_view.external_id)
                if source_type_key is not None and source_type_key in custom_views:
                    mapping = mapping.model_copy(update={"destination_view": custom_views[source_type_key]})
                remapped.append(mapping)
            mappings = remapped
        remapped_source: list[ViewToViewMapping] = []
        for mapping in mappings:
            source_entity = ENTITY_BY_SOURCE_VIEW_EXTERNAL_ID.get(mapping.source_view.external_id)
            if source_entity is not None and source_entity in source_views:
                mapping = mapping.model_copy(update={"source_view": source_views[source_entity]})
            remapped_source.append(mapping)
        mappings = remapped_source
        return mappings

    def _get_target_spaces(self, migration_spaces: InfieldMigrationSpaces) -> set[str]:
        target_spaces = (
            {migration_spaces.target} if not migration_spaces.is_location_split else self.lookup.target_spaces
        )
        return target_spaces

    @classmethod
    def get_infield_source_selectors(
        cls, migration_spaces: InfieldMigrationSpaces, mappings: list[ViewToViewMapping]
    ) -> list[InstanceSelector]:
        """Creates instance selectors for APM_SourceData migration based on the provided mappings and migration spaces."""
        return [
            InstanceViewSelector(
                view=SelectedView(
                    space=mapping.source_view.space,
                    external_id=mapping.source_view.external_id,
                    version=mapping.source_view.version,
                ),
                instance_spaces=(migration_spaces.source,),
                endpoint="sync",
            )
            for mapping in mappings
        ]

    def get_infield_source_mapper(
        self,
        migration_spaces: InfieldMigrationSpaces,
        mappings: list[ViewToViewMapping],
        source_views: dict[str, ViewId],
    ) -> FDMtoCDMMapper:
        """Creates a data mapper for APM_SourceData migration based on the provided mappings and migration spaces."""
        instance_id_mapper = self._create_instance_id_mappers(migration_spaces, passthrough={})
        apm_asset_properties = {"assetExternalId", "assetExternalIds"}
        custom_mappings: list[CustomConnectionMapping] = [
            InFieldAssetMapping(
                self.client,
                extra_asset_view_properties=[
                    (m.source_view, source_prop)
                    for m in mappings
                    for source_prop in m.container_mapping
                    if source_prop in apm_asset_properties
                ],
            ),
            APMSourceDataMaintenanceOrderMapping(
                migration_spaces.source,
                instance_id_mapper,
                resolved_operation_view=source_views.get("operation"),
            ),
        ]
        connection_creator = ConnectionCreator(
            self.client,
            instance_id_mapper=instance_id_mapper,
            custom_mappings=custom_mappings,
        )
        if not migration_spaces.is_location_split:
            return FDMtoCDMMapper(self.client, mappings, connection_creator=connection_creator)

        location_split_id_mapper = (
            instance_id_mapper if isinstance(instance_id_mapper, LocationSplitInstanceIdMapper) else None
        )
        if not location_split_id_mapper:
            raise RuntimeError(
                "Toolkit bug: LocationSplitInstanceIdMapper should be used for location split migrations."
            )

        return LocationSplitFDMtoCDMMapper(
            self.client,
            mappings,
            connection_creator,
            location_split_id_mapper,
            self.lookup.target_by_root_asset,
            source_views=source_views,
        )
