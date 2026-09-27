from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
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
    SpaceMappingInstanceIdMapper,
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
    SelectedView,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.ui import ToolkitPanel, ToolkitTable
from cognite_toolkit._cdf_tk.utils import humanize_collection


@dataclass
class InfieldMigrationSpaces:
    source: str
    target: str | None

    @property
    def is_location_split(self) -> bool:
        return self.target is None

class InFieldUserInput:
    def __init__(self, client: ToolkitClient, operation: str) -> None:
        self.client = client
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
            return InfieldMigrationSpaces(source=source_space, target=None)

        target_space = self._prompt_space(self._target_candidates, "target")
        return InfieldMigrationSpaces(source=source_space, target=target_space)

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

    def validate_migration_spaces(self, user_source_space: str, user_target_space: str | None) -> InfieldMigrationSpaces:
        self._is_valid_space(user_source_space, self._source_candidates, "source")
        if self._is_split_location(user_source_space):
            if user_target_space is not None:
                raise typer.BadParameter(
                    f"Source space {user_source_space!r} is shared by multiple InField locations; These must be split into "
                    "multiple target spaces during migration. You should rerun this command without --target-space so "
                    "Toolkit can determine the appropriate target spaces from the deployed location configs."
                )
            return InfieldMigrationSpaces(source=user_source_space, target=None)
        if user_target_space is None:
            raise typer.BadParameter(
                f"Target space must be provided for non-split {self.operation} migration. "
                f"Available target spaces are: {humanize_collection(self._target_candidates)}."
            )
        self._is_valid_space(user_target_space, self._target_candidates, "target")

        return InfieldMigrationSpaces(source=user_source_space, target=user_target_space)

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
        return Flags.INFIELD_LOCATION_SPLIT.is_enabled() and  source_space in self._shared_legacy_spaces

    @cached_property
    def _apm_configs(self) -> Sequence[APMConfigResponse]:
        return self.client.infield.apm_config.list(limit=None)

    @cached_property
    def _source_candidates(self) -> set[str]:
        return {
            location.app_data_instance_space
            for config in self._apm_configs
            if config.feature_configuration
            for location in config.feature_configuration.root_location_configurations or []
            if location.app_data_instance_space is not None
        }

    @property
    def _target_candidates(self) -> set[str]:
        infield_cdm_configs = self.client.infield.cdm_config.list(limit=None)
        return {
            config.data_storage.app_instance_space
            for config in infield_cdm_configs
            if config.data_storage and config.data_storage.app_instance_space
        }

    @cached_property
    def _shared_legacy_spaces(self) -> set[str]:
        return find_shared_legacy_instance_spaces(self._apm_configs)

    def _get_space_stats(self, spaces: set[str]) -> dict[str, SpaceStatistics]:
        return {stat.space: stat for stat in self.client.data_modeling.statistics.spaces.retrieve(list(spaces))}


class InFieldSetup:
    def __init__(self, client: ToolkitClient):
        self.client = client

    def get_infield_data_mapper(self, migration_spaces: InfieldMigrationSpaces) -> FDMtoCDMMapper:
        schedule_mapper = InFieldLegacyToCDMScheduleMapper(
            self.client, connection_creator, schedule_mapping, location_split_id_mapper
        )
        custom_instance_mappings = {schedule_mapper.SCHEDULE_VIEW: schedule_mapper}

        if migration_spaces.is_location_split:
            custom_instance_mappings[COGNITE_SOLUTION_TAG_VIEW_ID] = LocationSplitSolutionTagMapper(
                self.client, connection_creator, solution_tag_mapping, target_spaces
            )

            return LocationSplitFDMtoCDMMapper(
                self.client,
                infield_mappings,
                connection_creator,
                location_split_id_mapper,
                target_by_root_asset,
                custom_properties_mappings=custom_properties_mappings,
                custom_instance_mappings=custom_instance_mappings,
            )
        else:
            return FDMtoCDMMapper(
                self.client,
                infield_mappings,
                connection_creator=connection_creator,
                custom_properties_mappings=custom_properties_mappings,
                custom_instance_mappings=custom_instance_mappings,
            )


def validate_or_prompt_source_and_target_space(client: ToolkitClient, user_source_space: str | None, user_target_space: str | None, label: str) -> InfieldMigrationSpaces:
    """Resolve source and target spaces for Infield data migrations.

    With the infield-location-split alpha flag, shared source spaces skip ``--target-space``;
    targets come from deployed location configs.
    """

    source_candidates = ource_space and user_source_space not in source_candidates:
        raise typer.BadParameter(
            f"Source space '{user_source_space}' is not a valid source for {label} migration. "
            f"Available source spaces are: {humanize_collection(source_candidates)}."
        )
    if user_source_space is None and len(source_candidates) == 0:
        raise typer.BadParameter("No APM Configurations with app data space found. Cannot migrate Infield data. "
                                 "Are you sure you are an InField customer?")

    existing_source_candidates = {item.space: item for item in client.data_modeling.statistics.spaces.retrieve(list(source_candidates))}
    if not existing_source_candidates:
        raise typer.BadParameter(
            f"Source spaces {humanize_collection(source_candidates)} do not exist or cannot be accessed. "
            f"Please ensure the {label} instance space contains data and can be accessed."
        )


    shared_legacy_instance_spaces = find_shared_legacy_instance_spaces(apm_configs)

    infield_cdm_configs = client.infield.cdm_config.list(limit=None)
    target_candidates = {
        config.data_storage.app_instance_space
        for config in infield_cdm_configs
        if config.data_storage and config.data_storage.app_instance_space
    }

    if not target_candidates:
        raise typer.BadParameter(
            "No InfieldOnCDM Configurations with app instance space found. Cannot migrate Infield data. "
            "Have you placed the CDMLocationConfig YAMLs generated by 'cdf migrate infield-configs' "
            "into a Toolkit module and deployed them with 'cdf deploy'?"
        )

    if sum(1 for s in (source_space, target_space) if s is not None) == 1:
        raise typer.BadParameter("Either both --source-space and --target-space must be provided, or neither.")

    return InfieldMigrationSpaces(source=source_space, target=target_space)



def get_infield_data_selectors(client: ToolkitClient, source_space: str | None, target_space: str | None, skip_observations: bool) -> list[InstanceSelector]:

    shared_legacy_spaces = find_shared_legacy_instance_spaces(apm_configs)
    source_space, target_space = _resolve_infield_migration_spaces(
        client=client,
        source_space=source_space,
        target_space=target_space,
        source_candidates=source_candidates,
        target_candidates=target_candidates,
        shared_legacy_spaces=shared_legacy_spaces,
        label="Infield data",
    )

    instance_id_mapper, location_split_id_mapper, target_spaces, target_by_root_asset = (
        _infield_instance_id_mappers(
            client,
            source_space=source_space,
            target_space=target_space,
            shared_legacy_spaces=shared_legacy_spaces,
            apm_configs=apm_configs,
            cdm_configs=infield_cdm_configs,
            target_kind="app_data",
            passthrough_space_mapping={"cognite_app_data": "cognite_app_data"},  # users stay in this space
            label="Infield data",
        )
    )
    _print_location_split_plan(
        client,
        source_space=source_space,
        target_by_root_asset=target_by_root_asset,
        label="Infield data",
    )
    infield_mappings = create_infield_data_mappings()
    if skip_observations:
        # Skip the default mapping to the FieldObservation view if users will be using custom observation views.
        # If this skip is not done, users will end up with observations both in the custom observation view and the default FieldObservation view,
        # which can lead to the wrong view being rendered for migrated observations in Infield since it relies on the instances/inspect endpoint.
        infield_mappings = [m for m in infield_mappings if m.destination_view.external_id != "FieldObservation"]
    else:
        # If a custom observation view is configured for the target space (e.g. to support SAP writeback),
        # migrate Observations onto it instead of the default FieldObservation view.
        custom_observation_views = {
            resolve_observation_view_id(infield_cdm_configs, space) for space in target_spaces
        }
        if len(custom_observation_views) > 1:
            raise ToolkitMigrationError(
                "Location split targets disagree on the custom observation view. "
                f"Distinct views: {humanize_collection([str(view_id) for view_id in custom_observation_views])}."
            )
        custom_observation_view = next(iter(custom_observation_views), None)
        if custom_observation_view is not None:
            infield_mappings = [
                m.model_copy(update={"destination_view": custom_observation_view})
                if m.source_view.external_id == "Observation"
                else m
                for m in infield_mappings
            ]
    schedule_selector = create_infield_schedule_selector(instance_space=source_space)
    selectors: list[InstanceViewSelector | InstanceQuerySelector] = []
    schedule_mapping: ViewToViewMapping | None = None
    solution_tag_mapping: ViewToViewMapping | None = None
    for mapping in infield_mappings:
        if mapping.source_view.external_id == "Schedule":
            # Special case for schedules, see create_infield_schedule_query for docs on why.
            selectors.append(schedule_selector)
            schedule_mapping = mapping
            continue
        if mapping.source_view == COGNITE_SOLUTION_TAG_VIEW_ID:
            solution_tag_mapping = mapping
        edge_types = list(mapping.edge_mapping.keys()) if mapping.edge_mapping else []
        selectors.append(
            InstanceViewSelector(
                view=SelectedView(
                    space=mapping.source_view.space,
                    external_id=mapping.source_view.external_id,
                    version=mapping.source_view.version,
                ),
                instance_spaces=(source_space,),
                edge_types=tuple(dict.fromkeys(edge_types)) or None,
                endpoint="sync",
            )
        )
    if schedule_mapping is None:
        raise ValueError("No mapping for Schedule view found in infield_data_mappings.yaml")
    connection_creator = ConnectionCreator(
        client,
        instance_id_mapper=instance_id_mapper,
        custom_mappings=[InFieldAssetMapping(client)],
        direct_relation_edge_tiebreakers=DIRECT_RELATION_EDGE_TIEBREAKERS,
    )
    custom_properties_mappings = [
        InFieldConditionMapping(infield_mappings),
        InFieldUserMapping(),
        InFieldObservationSapStatusMapping(),
    ]



def get_infield_data_mapper(client: ToolkitClient) -> FDMtoCDMMapper:
    mapper: FDMtoCDMMapper
    schedule_mapper = InFieldLegacyToCDMScheduleMapper(
        client, connection_creator, schedule_mapping, location_split_id_mapper
    )
    if location_split_id_mapper is not None:
        if solution_tag_mapping is None:
            raise ValueError("No mapping for CogniteSolutionTag view found in infield_data_mappings.yaml")
        return LocationSplitFDMtoCDMMapper(
            client,
            infield_mappings,
            connection_creator,
            location_split_id_mapper,
            target_by_root_asset,
            custom_properties_mappings=custom_properties_mappings,
            custom_instance_mappings={
                InFieldLegacyToCDMScheduleMapper.SCHEDULE_VIEW: schedule_mapper,
                COGNITE_SOLUTION_TAG_VIEW_ID: LocationSplitSolutionTagMapper(
                    client, connection_creator, solution_tag_mapping, target_spaces
                ),
            },
        )
    else:
        return FDMtoCDMMapper(
            client,
            infield_mappings,
            connection_creator=connection_creator,
            custom_properties_mappings=custom_properties_mappings,
            custom_instance_mappings={InFieldLegacyToCDMScheduleMapper.SCHEDULE_VIEW: schedule_mapper},
        )


def get_infield_source_data_selectors(client: ToolkitClient, source_space: str | None, target_space: str | None) -> list[InstanceSelector]:
    apm_configs = client.infield.apm_config.list(limit=None)
    source_candidates = resolve_apm_source_data_instance_spaces(apm_configs)
    infield_cdm_configs = client.infield.cdm_config.list(limit=None)
    target_candidates = {
        space
        for config in infield_cdm_configs
        for type_key in SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID.values()
        if (space := get_first_instance_space(config.data_filters, type_key)) is not None
    }
    if not source_candidates:
        raise typer.BadParameter(
            "No APM Configurations with sourceDataInstanceSpace found. Cannot migrate APM_SourceData."
        )
    if not target_candidates:
        raise typer.BadParameter(
            "No InfieldOnCDM Configurations with maintenanceOrders/operations/notifications dataFilters found. "
            "Cannot migrate APM_SourceData. Have you placed the CDMLocationConfig YAMLs generated by "
            "'cdf migrate infield-configs' into a Toolkit module and deployed them with 'cdf deploy'?"
        )
    shared_legacy_spaces = find_shared_legacy_instance_spaces(apm_configs)
    source_space, target_space, log_dir, dry_run, verbose = _resolve_infield_migration_spaces(
        client=client,
        source_space=source_space,
        target_space=target_space,
        source_candidates=source_candidates,
        target_candidates=target_candidates,
        shared_legacy_spaces=shared_legacy_spaces,
        log_dir=log_dir,
        dry_run=dry_run,
        verbose=verbose,
        label="APM_SourceData",
    )

    source_views = resolve_apm_source_data_view_ids(apm_configs)
    instance_id_mapper, location_split_id_mapper, target_spaces, target_by_root_asset = (
        _infield_instance_id_mappers(
            client,
            source_space=source_space,
            target_space=target_space,
            shared_legacy_spaces=shared_legacy_spaces,
            apm_configs=apm_configs,
            cdm_configs=infield_cdm_configs,
            target_kind="source_data",
            label="APM_SourceData",
        )
    )
    _print_location_split_plan(
        client,
        source_space=source_space,
        target_by_root_asset=target_by_root_asset,
        label="APM_SourceData",
    )

    mappings = create_apm_source_data_mappings()
    custom_views: dict[str, ViewId | None] = {}
    for space in target_spaces:
        space_custom_views, custom_view_warnings = resolve_source_data_view_ids(infield_cdm_configs, space)
        for warning in custom_view_warnings:
            client.console.print(
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
    selectors: list[InstanceViewSelector] = [
        InstanceViewSelector(
            view=SelectedView(
                space=mapping.source_view.space,
                external_id=mapping.source_view.external_id,
                version=mapping.source_view.version,
            ),
            instance_spaces=(source_space,),
            endpoint="sync",
        )
        for mapping in mappings
    ]
    apm_asset_properties = {"assetExternalId", "assetExternalIds"}
    custom_mappings: list[CustomConnectionMapping] = [
        InFieldAssetMapping(
            client,
            extra_asset_view_properties=[
                (m.source_view, source_prop)
                for m in mappings
                for source_prop in m.container_mapping
                if source_prop in apm_asset_properties
            ],
        ),
        APMSourceDataMaintenanceOrderMapping(
            source_space,
            instance_id_mapper,
            resolved_operation_view=source_views.get("operation"),
        ),
    ]
    connection_creator = ConnectionCreator(
        client,
        instance_id_mapper=instance_id_mapper,
        custom_mappings=custom_mappings,
    )
    mapper: FDMtoCDMMapper
    if location_split_id_mapper is not None:
        mapper = LocationSplitFDMtoCDMMapper(
            client,
            mappings,
            connection_creator,
            location_split_id_mapper,
            target_by_root_asset,
            source_views=source_views,
        )
    else:
        mapper = FDMtoCDMMapper(client, mappings, connection_creator=connection_creator)

def get_infield_source_data_mapper() -> FDMtoCDMMapper:
    raise NotImplementedError()



def _is_location_split_source(source_space: str | None, shared_legacy_spaces: set[str]) -> bool:
    return (
        source_space is not None
        and Flags.INFIELD_LOCATION_SPLIT.is_enabled()
        and source_space in shared_legacy_spaces
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


def _infield_instance_id_mappers(
    client: ToolkitClient,
    *,
    source_space: str,
    target_space: str | None,
    shared_legacy_spaces: set[str],
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    target_kind: LocationSplitKind,
    label: str,
    passthrough_space_mapping: Mapping[str, str] | None = None,
) -> tuple[InstanceIdMapper, LocationSplitInstanceIdMapper | None, set[str], dict[str, str]]:
    passthrough = dict(passthrough_space_mapping or {})
    if _is_location_split_source(source_space, shared_legacy_spaces):
        target_by_root_asset = build_target_by_root_asset(
            client,
            source_space=source_space,
            apm_configs=apm_configs,
            cdm_configs=cdm_configs,
            target_kind=target_kind,
        )
        target_spaces = set(target_by_root_asset.values())
        location_split_id_mapper = LocationSplitInstanceIdMapper(
            client,
            source_space,
            passthrough_space_mapping=passthrough or None,
            target_spaces=target_spaces,
        )
        return (
            location_split_id_mapper,
            location_split_id_mapper,
            target_spaces,
            target_by_root_asset,
        )
    if target_space is None:
        raise typer.BadParameter(f"Bug in Toolkit: target space is required for non-split {label} migration.")
    instance_id_mapper = SpaceMappingInstanceIdMapper({source_space: target_space, **passthrough})
    return instance_id_mapper, None, {target_space}, {}


def _resolve_infield_migration_spaces(
    *,
    client: ToolkitClient,
    source_space: str | None,
    target_space: str | None,
    source_candidates: set[str],
    target_candidates: set[str],
    shared_legacy_spaces: set[str],
    label: str,
) -> tuple[str, str | None]:
    """Select/validate source (and optionally target) spaces for Infield data migrations.

    With the infield-location-split alpha flag, shared source spaces skip ``--target-space``;
    targets come from deployed location configs.
    """
    if source_space is None and target_space is None:
        source_stats = client.data_modeling.statistics.spaces.retrieve(list(source_candidates))
        if not source_stats:
            raise typer.BadParameter(
                f"Source spaces {humanize_collection(source_candidates)} do not exist or cannot be accessed. "
                f"Please ensure the {label} instance space contains data and can be accessed."
            )
        source_space = questionary.select(
            f"Select the instance space to migrate {label} from:",
            choices=[
                questionary.Choice(
                    title=f"{item.space} (contains {item.nodes:,} nodes and {item.edges:,} edges)",
                    value=item.space,
                )
                for item in source_stats
            ],
        ).unsafe_ask()
        if not isinstance(source_space, str):
            raise typer.BadParameter(f"No source space selected for {label} migration.")
        if _is_location_split_source(source_space, shared_legacy_spaces):
            # Cannot specify target space if this is a location split migration
            target_space = None
        else:
            target_stats = client.data_modeling.statistics.spaces.retrieve(list(target_candidates))
            if not target_stats:
                raise typer.BadParameter(
                    f"Target spaces {humanize_collection(target_candidates)} do not exist or cannot be accessed. "
                    "Please create the instance space or ensure you can access it."
                )
            target_space = questionary.select(
                f"Select the instance space to migrate {label} to:",
                choices=[
                    questionary.Choice(
                        title=f"{item.space} (contains {item.nodes:,} nodes and {item.edges:,} edges)",
                        value=item.space,
                    )
                    for item in target_stats
                ],
            ).unsafe_ask()

        return source_space, target_space

    if source_space is not None and source_space not in source_candidates:
        raise typer.BadParameter(
            f"Source space '{source_space}' is not a valid source for {label} migration. "
            f"Available source spaces are: {humanize_collection(source_candidates)}."
        )

    if source_space is not None and target_space is not None:
        if _is_location_split_source(source_space, shared_legacy_spaces):
            raise typer.BadParameter(
                f"Source space {source_space!r} is shared by multiple InField locations; These must be split into "
                "multiple target spaces during migration. You should rerun this command without --target-space so "
                "Toolkit can determine the appropriate target spaces from the deployed location configs."
            )
        if target_space not in target_candidates:
            raise typer.BadParameter(
                f"Target space '{target_space}' is not a valid target for {label} migration. "
                f"Available target spaces are: {humanize_collection(target_candidates)}."
            )
        return source_space, target_space

    if (
        source_space is not None
        and target_space is None
        and _is_location_split_source(source_space, shared_legacy_spaces)
    ):
        return source_space, None

    raise typer.BadParameter("Either both --source-space and --target-space must be provided, or neither.")

