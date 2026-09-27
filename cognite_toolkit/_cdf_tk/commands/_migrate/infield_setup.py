from collections.abc import Sequence, Mapping
from dataclasses import dataclass
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Any, Literal

import questionary
import typer
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics
from rich.console import Group
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import (
    ENTITY_BY_SOURCE_VIEW_EXTERNAL_ID,
    SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID,
    create_apm_source_data_mappings,
    get_first_instance_space,
    resolve_apm_source_data_instance_spaces,
    resolve_apm_source_data_view_ids,
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
    LocationSplitInstanceIdMapper,
    SpaceMappingInstanceIdMapper, InstanceIdMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
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
    InstanceQuerySelector,
    InstanceViewSelector,
    SelectedView, InstanceSelector,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.ui import ToolkitPanel, ToolkitTable
from cognite_toolkit._cdf_tk.utils import humanize_collection


@dataclass
class InfieldMigrationSpaces:
    source: str
    _target: str | None

    @property
    def target(self) -> str:
        if self._target is None:
            raise ValueError("Target space is None, cannot access target_space property.")
        return self._target

    @property
    def is_location_split(self) -> bool:
        return self._target is None

class InFieldLookup:
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
            target_kind={"Infield data": "app_data", "APM_SourceData": "source_data"}[self.operation],
        )

    @cached_property
    def target_spaces(self) -> set[str]:
        return set(self.target_by_root_asset(self.source_space).values())

    @cached_property
    def shared_legacy_spaces(self) -> set[str]:
        return find_shared_legacy_instance_spaces(self.apm_configs)

class InFieldUserInput:
    def __init__(self, client: ToolkitClient, lookup: InFieldLookup, operation: Literal["Infield data", "APM_SourceData"]) -> None:
        self.client = client
        self.lookup = lookup
        self.operation = operation

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

    def validate_migration_spaces(self, user_source_space: str, user_target_space: str | None) -> InfieldMigrationSpaces:
        self._is_valid_space(user_source_space, self._source_candidates, "source")
        if self._is_split_location(user_source_space):
            if user_target_space is not None:
                raise typer.BadParameter(
                    f"Source space {user_source_space!r} is shared by multiple InField locations; These must be split into "
                    "multiple target spaces during migration. You should rerun this command without --target-space so "
                    "Toolkit can determine the appropriate target spaces from the deployed location configs."
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
                f"The following {space_label} spaces do not exist or cannot be accessed: {humanize_collection(missing)}.").print_warning(
                console=self.client.console)
        stats = [existing_candidates[space] for space in candidates if
                        space in existing_candidates]
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

    def _is_valid_space(self, user_space: str, candidates: set[str], space_label: Literal["source", "target"]):
        """Checks if the user-provided space is valid and exists in the candidates. Raises a BadParameter exception if not."""
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
        return Flags.INFIELD_LOCATION_SPLIT.is_enabled() and  source_space in self.lookup.shared_legacy_spaces

    @cached_property
    def _source_candidates(self) -> set[str]:
        return {
            location.app_data_instance_space
            for config in self.lookup.apm_configs
            if config.feature_configuration
            for location in config.feature_configuration.root_location_configurations or []
            if location.app_data_instance_space is not None
        }

    @property
    def _target_candidates(self) -> set[str]:
        return {
            config.data_storage.app_instance_space
            for config in self.lookup.cdm_configs
            if config.data_storage and config.data_storage.app_instance_space
        }

    def _get_space_stats(self, spaces: set[str]) -> dict[str, SpaceStatistics]:
        return {stat.space: stat for stat in self.client.data_modeling.statistics.spaces.retrieve(list(spaces))}


class InFieldSetup:
    def __init__(self, client: ToolkitClient, lookup: InFieldLookup, skip_observations: bool = False) -> None:
        self.client = client
        self.lookup = lookup
        self.skip_observations = skip_observations

    def create_instance_id_mappers(self, migration_spaces: InfieldMigrationSpaces, passthrough: dict[str, str]) -> InstanceIdMapper:
        if not migration_spaces.is_location_split:
            return SpaceMappingInstanceIdMapper({migration_spaces.source: migration_spaces.target, **passthrough})

        return  LocationSplitInstanceIdMapper(
            self.client,
            migration_spaces.source,
            passthrough_space_mapping=passthrough or None,
            target_spaces=self.lookup.target_spaces,
        )

    def infield_mappings(self) -> list[ViewToViewMapping]:
        mappings = create_infield_data_mappings()
        if self.skip_observations:
            # Skip the default mapping to the FieldObservation view if users will be using custom observation views.
            # If this skip is not done, users will end up with observations both in the custom observation view and the default FieldObservation view,
            # which can lead to the wrong view being rendered for migrated observations in Infield since it relies on the instances/inspect endpoint.
            return [mapping for mapping in mappings if mapping.destination_view.external_id != "FieldObservation"]

        # If a custom observation view is configured for the target space (e.g. to support SAP writeback),
        # migrate Observations onto it instead of the default FieldObservation view.
        custom_observation_views = {resolve_observation_view_id(self.lookup.cdm_configs, space) for space in self.lookup.target_spaces}
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
    def get_infield_data_selectors(cls, migration_spaces: InfieldMigrationSpaces, infield_mappings: list[ViewToViewMapping]) -> list[InstanceSelector]:
        selectors: list[InstanceViewSelector | InstanceQuerySelector] = []
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

    def get_infield_data_mapper(self, migration_spaces: InfieldMigrationSpaces, infield_mappings: list[ViewToViewMapping], instance_id_mapper: InstanceIdMapper) -> FDMtoCDMMapper:
        location_split_id_mapper = instance_id_mapper if isinstance(instance_id_mapper, LocationSplitInstanceIdMapper) else None
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
        schedule_mapper = InFieldLegacyToCDMScheduleMapper(
            self.client, connection_creator, schedule_mapping, location_split_id_mapper
        )
        custom_instance_mappings = {schedule_mapper.SCHEDULE_VIEW: schedule_mapper}

        if not migration_spaces.is_location_split:
            return FDMtoCDMMapper(
                self.client,
                infield_mappings,
                connection_creator=connection_creator,
                custom_properties_mappings=custom_properties_mappings,
                custom_instance_mappings=custom_instance_mappings,
            )
        if not location_split_id_mapper:
            raise RuntimeError("Toolkit bug: LocationSplitInstanceIdMapper should be used for location split migrations.")

        custom_instance_mappings[COGNITE_SOLUTION_TAG_VIEW_ID] = LocationSplitSolutionTagMapper(
            self.client, connection_creator, solution_tag_mapping, self.lookup.target_spaces
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





def _print_location_split_plan(
    client: ToolkitClient,
    *,
    source_space: str,
    target_by_root_asset: Mapping[str, str],
    label: str,
) -> None:
    """Show the resolved root-location -> target-space plan."""
    if not target_by_root_asset:
        return
    table = ToolkitTable("Root location", "Target instance space")
    for root_asset, target_space in sorted(target_by_root_asset.items()):
        table.add_row(root_asset, target_space)
    client.console.print(
        ToolkitPanel(
            Group(
                f"Legacy instance space [bold]{source_space!r}[/] is shared by these root locations. "
                f"{label} will be split into the following target instance spaces:",
                table.as_panel_detail(),
            ),
            title="Location split plan",
        )
    )
