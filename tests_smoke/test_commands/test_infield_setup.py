from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Literal, cast
from unittest.mock import MagicMock, patch

import pytest
import typer
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.apps._migrate_app import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigResponse,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import DataStorage, InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.commands._migrate import infield_setup
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import resolve_apm_source_data_view_ids
from cognite_toolkit._cdf_tk.commands._migrate.conversion import InstanceMappingError
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import FDMtoCDMMapper
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    DIRECT_RELATION_EDGE_TIEBREAKERS,
    create_infield_data_mappings,
    create_infield_schedule_selector,
)
from cognite_toolkit._cdf_tk.commands._migrate.infield_setup import (
    InFieldLookup,
    InfieldMigrationSpaces,
    InFieldSetup,
    InFieldUserInput,
)
from cognite_toolkit._cdf_tk.commands._migrate.location_split import COGNITE_SOLUTION_TAG_VIEW_ID
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceViewSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.tk_warnings.other import HighSeverityWarning

Operation = Literal["Infield data", "APM_SourceData"]

CUSTOM_OBSERVATION_VIEW = ViewId(space="sp_customer_idm", external_id="ObservationView", version="v1")
OTHER_OBSERVATION_VIEW = ViewId(space="sp_customer_idm", external_id="OtherObservationView", version="v1")
CUSTOM_ACTIVITY_VIEW = ViewId(space="sp_customer_idm", external_id="CustomActivity", version="v9")
CUSTOM_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="CustomOperation", version="v2")
OTHER_OPERATION_VIEW = ViewId(space="sp_customer_idm", external_id="OtherOperation", version="v2")

SOURCE_DATA_FILTERS: dict[str, JsonValue] = {
    "maintenanceOrders": {"instanceSpaces": ["cdm_source"]},
    "operations": {"instanceSpaces": ["cdm_source"]},
    "notifications": {"instanceSpaces": ["cdm_source"]},
}


@pytest.fixture(autouse=True)
def _location_split_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_location_split(monkeypatch, False)


def _set_location_split(monkeypatch: pytest.MonkeyPatch, enabled: bool) -> None:
    monkeypatch.setattr(
        FeatureFlag,
        "is_enabled",
        lambda flag: enabled and flag is Flags.INFIELD_LOCATION_SPLIT,
    )


def _root(
    external_id: str,
    asset_external_id: str,
    app_data_instance_space: str | None = None,
    source_data_instance_space: str | None = None,
) -> RootLocationConfiguration:
    return RootLocationConfiguration(
        external_id=external_id,
        asset_external_id=asset_external_id,
        app_data_instance_space=app_data_instance_space,
        source_data_instance_space=source_data_instance_space,
    )


def _apm(
    *roots: RootLocationConfiguration,
    customer_data_space_id: str | None = None,
    view_mappings: dict[str, JsonValue] | None = None,
    external_id: str = "APP_CONFIG_V2",
) -> APMConfigResponse:
    return APMConfigResponse(
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        customer_data_space_id=customer_data_space_id,
        feature_configuration=FeatureConfiguration(
            root_location_configurations=list(roots) or None,
            view_mappings=view_mappings,
        ),
    )


def _cdm(
    external_id: str,
    app_instance_space: str | None = None,
    data_filters: dict[str, JsonValue] | None = None,
    view_mappings: dict[str, JsonValue] | None = None,
) -> InFieldCDMLocationConfigResponse:
    return InFieldCDMLocationConfigResponse(
        instance_type="node",
        space="sp_instance",
        external_id=external_id,
        version=1,
        created_time=0,
        last_updated_time=0,
        data_storage=DataStorage(app_instance_space=app_instance_space) if app_instance_space is not None else None,
        data_filters=data_filters,
        view_mappings=view_mappings,
    )


def _observation_mappings(view: ViewId) -> dict[str, JsonValue]:
    return {"observation": [{"view": {"space": view.space, "externalId": view.external_id, "version": view.version}}]}


def _source_view_mapping(view: ViewId) -> dict[str, JsonValue]:
    mapping: dict[str, JsonValue] = {
        "space": view.space,
        "externalId": view.external_id,
        "version": view.version,
    }
    return mapping


def _mock_client(
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    existing_spaces: set[str] | None = None,
) -> ToolkitClient:
    client = MagicMock()
    client.infield.apm_config.list.return_value = list(apm_configs)
    client.infield.cdm_config.list.return_value = list(cdm_configs)

    def retrieve(spaces: list[str]) -> list[SimpleNamespace]:
        selected = spaces if existing_spaces is None else [space for space in spaces if space in existing_spaces]
        return [SimpleNamespace(space=space, nodes=4, edges=1) for space in selected]

    client.data_modeling.statistics.spaces.retrieve.side_effect = retrieve
    return cast(ToolkitClient, client)


def _user_input(
    operation: Operation,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    existing_spaces: set[str] | None = None,
) -> InFieldUserInput:
    client = _mock_client(apm_configs, cdm_configs, existing_spaces)
    return InFieldUserInput(client, InFieldLookup(client, operation))


def _setup(
    operation: Operation,
    apm_configs: Sequence[APMConfigResponse] | None = None,
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse] | None = None,
    existing_spaces: set[str] | None = None,
) -> tuple[InFieldSetup, InFieldLookup]:
    client = _mock_client(apm_configs or [], cdm_configs or [], existing_spaces)
    lookup = InFieldLookup(client, operation)
    return InFieldSetup(client, lookup), lookup


def _patch_targets(monkeypatch: pytest.MonkeyPatch, targets: dict[str, str] | None = None) -> dict[str, str]:
    seen: dict[str, str] = {}
    resolved = targets or {"ASSET_1": "cdm_a", "ASSET_2": "cdm_b"}

    def _fake(
        client: ToolkitClient,
        *,
        source_space: str,
        apm_configs: object,
        cdm_configs: object,
        target_kind: str,
    ) -> dict[str, str]:
        seen["target_kind"] = target_kind
        seen["source_space"] = source_space
        return resolved

    monkeypatch.setattr(infield_setup, "build_target_by_root_asset", _fake)
    return seen


def _mapped_space(mapper: FDMtoCDMMapper, space: str) -> str | None:
    try:
        return mapper._connection_creator._instance_id_mapper.map_instance_id(
            NodeId(space=space, external_id="item")
        ).space
    except (InstanceMappingError, RuntimeError):
        return None


class _Prompts:
    def __init__(self, selections: list[object]) -> None:
        self._selections = list(selections)
        self.messages: list[str] = []
        self.path_defaults: list[str] = []
        self.confirm_defaults: list[bool] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> "_Prompts":
        monkeypatch.setattr(infield_setup.questionary, "select", self._select)
        monkeypatch.setattr(infield_setup.questionary, "path", self._path)
        monkeypatch.setattr(infield_setup.questionary, "confirm", self._confirm)
        return self

    def _select(self, message: str, choices: list[object]) -> MagicMock:
        self.messages.append(message)
        value = self._selections.pop(0) if self._selections else None
        asked = MagicMock()
        asked.unsafe_ask.return_value = value
        return asked

    def _path(self, message: str, default: str) -> MagicMock:
        self.path_defaults.append(default)
        asked = MagicMock()
        asked.unsafe_ask.return_value = default
        return asked

    def _confirm(self, message: str, default: bool) -> MagicMock:
        self.confirm_defaults.append(default)
        asked = MagicMock()
        asked.unsafe_ask.return_value = default
        return asked


def _standard_infield_configs() -> tuple[list[APMConfigResponse], list[InFieldCDMLocationConfigResponse]]:
    return (
        [_apm(_root("loc", "ASSET_1", app_data_instance_space="app_space", source_data_instance_space="source_space"))],
        [_cdm("loc", app_instance_space="cdm_app", data_filters=SOURCE_DATA_FILTERS)],
    )


def _shared_apm() -> APMConfigResponse:
    return _apm(
        _root("loc1", "ASSET_1", app_data_instance_space="shared_app", source_data_instance_space="shared_source"),
        _root("loc2", "ASSET_2", app_data_instance_space="shared_app", source_data_instance_space="shared_source"),
    )


class TestInFieldSpaceSelection:
    def test_infield_data_accepts_app_spaces(self) -> None:
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)

        assert user_input.validate_migration_spaces("app_space", "cdm_app") == InfieldMigrationSpaces(
            source="app_space", _target="cdm_app"
        )

    def test_infield_data_rejects_source_data_space(self) -> None:
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)

        with pytest.raises(typer.BadParameter, match="Source space 'source_space' is not a valid source"):
            user_input.validate_migration_spaces("source_space", "cdm_app")

    @pytest.mark.parametrize(
        "space_label, user_space, match",
        [
            pytest.param(
                "source", "missing_source", "Source space 'missing_source' is not a valid source", id="source"
            ),
            pytest.param(
                "target", "missing_target", "Target space 'missing_target' is not a valid target", id="target"
            ),
        ],
    )
    def test_invalid_space_names_match_legacy_messages(self, space_label: str, user_space: str, match: str) -> None:
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)
        source, target = ("app_space", user_space) if space_label == "target" else (user_space, "cdm_app")

        with pytest.raises(typer.BadParameter, match=match):
            user_input.validate_migration_spaces(source, target)

    def test_location_split_accepts_source_without_target(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_location_split(monkeypatch, True)
        user_input = _user_input("Infield data", [_shared_apm()], [_cdm("loc", app_instance_space="cdm_app")])

        assert user_input.validate_migration_spaces("shared_app", None) == InfieldMigrationSpaces(
            source="shared_app", _target=None
        )

    def test_location_split_rejects_explicit_target(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_location_split(monkeypatch, True)
        user_input = _user_input("Infield data", [_shared_apm()], [_cdm("loc", app_instance_space="cdm_app")])

        with pytest.raises(typer.BadParameter, match="shared by multiple InField locations"):
            user_input.validate_migration_spaces("shared_app", "cdm_app")

    def test_interactive_selects_source_and_target(self, monkeypatch: pytest.MonkeyPatch) -> None:
        prompts = _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)

        assert (
            user_input.prompt_migration_spaces(),
            len(prompts.messages),
        ) == (InfieldMigrationSpaces(source="app_space", _target="cdm_app"), 2)

    def test_interactive_location_split_skips_target_prompt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_location_split(monkeypatch, True)
        prompts = _Prompts(["shared_app"]).install(monkeypatch)
        user_input = _user_input("Infield data", [_shared_apm()], [_cdm("loc", app_instance_space="cdm_app")])

        assert (user_input.prompt_migration_spaces(), len(prompts.messages)) == (
            InfieldMigrationSpaces(source="shared_app", _target=None),
            1,
        )

    def test_prompt_flags_returns_answers(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        prompts = _Prompts([]).install(monkeypatch)
        log_dir = tmp_path / "migration_logs"

        assert InFieldUserInput.prompt_flags(log_dir, dry_run=False, verbose=True) == (log_dir, False, True)
        assert prompts.confirm_defaults == [False, True]

    @pytest.mark.parametrize(
        "operation, source, target, apm_configs, cdm_configs",
        [
            pytest.param(
                "APM_SourceData",
                "source_space",
                "cdm_source",
                [_apm(_root("loc", "ASSET_1", "app_space", "source_space"))],
                [_cdm("loc", "cdm_app", SOURCE_DATA_FILTERS)],
                id="explicit_source_data_space_and_filter_target",
            ),
            pytest.param(
                "APM_SourceData",
                "customer_space",
                "cdm_source",
                [_apm(_root("loc", "ASSET_1", "app_space"), customer_data_space_id="customer_space")],
                [_cdm("loc", "cdm_app", SOURCE_DATA_FILTERS)],
                id="customer_data_space_fallback",
            ),
            pytest.param(
                "APM_SourceData",
                "customer_space",
                "cdm_source",
                [_apm(customer_data_space_id="customer_space")],
                [_cdm("loc", "cdm_app", SOURCE_DATA_FILTERS)],
                id="customer_data_space_without_root_locations",
            ),
        ],
    )
    def test_apm_source_data_accepts_legacy_source_and_target_spaces(
        self,
        operation: Operation,
        source: str,
        target: str,
        apm_configs: list[APMConfigResponse],
        cdm_configs: list[InFieldCDMLocationConfigResponse],
    ) -> None:
        user_input = _user_input(operation, apm_configs, cdm_configs, {source, target, "app_space", "cdm_app"})

        try:
            spaces = user_input.validate_migration_spaces(source, target)
        except typer.BadParameter as exc:
            pytest.fail(f"Expected {source!r} -> {target!r} to be accepted for {operation}, got {exc}")

        assert spaces == InfieldMigrationSpaces(source=source, _target=target)

    def test_apm_source_data_rejects_app_data_spaces(self) -> None:
        user_input = _user_input(
            "APM_SourceData",
            [_apm(_root("loc", "ASSET_1", "app_space", "source_space"))],
            [_cdm("loc", "cdm_app", SOURCE_DATA_FILTERS)],
        )

        with pytest.raises(typer.BadParameter, match="not a valid source"):
            user_input.validate_migration_spaces("app_space", "cdm_app")

    def test_blank_app_data_instance_space_is_not_a_candidate(self) -> None:
        user_input = _user_input(
            "Infield data",
            [_apm(_root("blank", "ASSET_0", ""), _root("loc", "ASSET_1", "app_space"))],
            [_cdm("loc", "cdm_app")],
            {"", "app_space", "cdm_app"},
        )

        with pytest.raises(typer.BadParameter, match="not a valid source"):
            user_input.validate_migration_spaces("", "cdm_app")

    @pytest.mark.parametrize(
        "operation, source, target, apm_configs, cdm_configs, match",
        [
            pytest.param(
                "Infield data",
                "app_space",
                "cdm_app",
                [],
                [],
                "No APM Configurations with app data space found",
                id="infield_data_without_source_spaces",
            ),
            pytest.param(
                "Infield data",
                "app_space",
                "cdm_app",
                [_apm(_root("loc", "ASSET_1", "app_space"))],
                [],
                "No InfieldOnCDM Configurations with app instance space found",
                id="infield_data_without_target_spaces",
            ),
            pytest.param(
                "APM_SourceData",
                "source_space",
                "cdm_source",
                [],
                [],
                "No APM Configurations with sourceDataInstanceSpace found",
                id="source_data_without_source_spaces",
            ),
            pytest.param(
                "APM_SourceData",
                "app_space",
                "cdm_source",
                [_apm(_root("loc", "ASSET_1", "app_space", "source_space"))],
                [],
                "maintenanceOrders/operations/notifications dataFilters",
                id="source_data_without_target_spaces",
            ),
        ],
    )
    def test_missing_configuration_uses_legacy_error(
        self,
        operation: Operation,
        source: str,
        target: str,
        apm_configs: list[APMConfigResponse],
        cdm_configs: list[InFieldCDMLocationConfigResponse],
        match: str,
    ) -> None:
        user_input = _user_input(operation, apm_configs, cdm_configs)

        with pytest.raises(typer.BadParameter, match=match):
            user_input.validate_migration_spaces(source, target)

    @pytest.mark.parametrize(
        "operation, match",
        [
            pytest.param(
                "Infield data",
                "No APM Configurations with app data space found",
                id="infield_data",
            ),
            pytest.param(
                "APM_SourceData",
                "No APM Configurations with sourceDataInstanceSpace found",
                id="apm_source_data",
            ),
        ],
    )
    def test_interactive_missing_configuration_uses_legacy_error(
        self, operation: Operation, match: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _Prompts(["unused", "unused"]).install(monkeypatch)
        user_input = _user_input(operation, [], [])

        with pytest.raises(typer.BadParameter, match=match):
            user_input.prompt_migration_spaces()

    def test_location_split_still_requires_target_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_location_split(monkeypatch, True)
        user_input = _user_input("Infield data", [_shared_apm()], [])

        with pytest.raises(typer.BadParameter, match="No InfieldOnCDM Configurations with app instance space found"):
            user_input.validate_migration_spaces("shared_app", None)

    def test_cli_accepts_configured_spaces_missing_from_statistics(self) -> None:
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs, existing_spaces=set())

        try:
            spaces = user_input.validate_migration_spaces("app_space", "cdm_app")
        except typer.BadParameter as exc:
            pytest.fail(
                f"Legacy CLI validation accepted configured spaces without checking CDF space statistics, got {exc}"
            )

        assert spaces == InfieldMigrationSpaces(source="app_space", _target="cdm_app")

    def test_source_without_target_requires_both_arguments(self) -> None:
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)

        with pytest.raises(
            typer.BadParameter,
            match="Either both --source-space and --target-space must be provided, or neither",
        ):
            user_input.validate_migration_spaces("app_space", None)

    def test_interactive_raises_when_source_spaces_are_inaccessible(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs, existing_spaces=set())

        with pytest.raises(typer.BadParameter, match="do not exist or cannot be accessed"):
            user_input.prompt_migration_spaces()

    def test_interactive_raises_when_target_spaces_are_inaccessible(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs, existing_spaces={"app_space"})

        with pytest.raises(typer.BadParameter, match="Please create the instance space or ensure you can access it"):
            user_input.prompt_migration_spaces()

    def test_interactive_does_not_warn_about_partially_missing_spaces(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        printed: list[str] = []

        def _spy(self: HighSeverityWarning, include_timestamp: bool = False, console: object = None) -> None:
            printed.append(self.get_message())

        monkeypatch.setattr(HighSeverityWarning, "print_warning", _spy)
        user_input = _user_input(
            "Infield data",
            [_apm(_root("loc1", "ASSET_1", "app_space"), _root("loc2", "ASSET_2", "other_space"))],
            [_cdm("loc", "cdm_app")],
            {"app_space", "cdm_app"},
        )

        user_input.prompt_migration_spaces()

        assert printed == []

    @pytest.mark.parametrize(
        "operation, legacy_label",
        [
            pytest.param("Infield data", "Infield data", id="infield_data"),
            pytest.param("APM_SourceData", "APM_SourceData", id="apm_source_data"),
        ],
    )
    def test_interactive_prompt_text_matches_legacy(
        self, operation: Operation, legacy_label: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        prompts = _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input(operation, apm_configs, cdm_configs)

        user_input.prompt_migration_spaces()

        assert prompts.messages == [
            f"Select the instance space to migrate {legacy_label} from:",
            f"Select the instance space to migrate {legacy_label} to:",
        ]

    def test_prompt_flags_uses_plain_log_dir_string(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        prompts = _Prompts([]).install(monkeypatch)
        log_dir = tmp_path / "nested"

        InFieldUserInput.prompt_flags(log_dir, dry_run=False, verbose=False)

        assert prompts.path_defaults == [str(log_dir)]

    def test_cancelled_target_prompt_uses_legacy_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _Prompts(["app_space", None]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        user_input = _user_input("Infield data", apm_configs, cdm_configs)
        setup = InFieldSetup(user_input.client, user_input.lookup)

        with pytest.raises(
            typer.BadParameter,
            match="Bug in Toolkit: target space is required for non-split Infield data migration",
        ):
            spaces = user_input.prompt_migration_spaces()
            user_input.lookup.source_space = spaces.source
            setup.get_infield_data_mapper(spaces, setup.infield_mappings(spaces))


class TestInFieldMappingsAndSelectors:
    def test_skip_observations_drops_field_observation_mapping(self) -> None:
        setup, _lookup = _setup(
            "Infield data",
            cdm_configs=[_cdm("loc", "cdm_app", view_mappings=_observation_mappings(CUSTOM_OBSERVATION_VIEW))],
        )
        mappings = setup.infield_mappings(InfieldMigrationSpaces(source="app_space", _target="cdm_app"), True)

        assert [
            (mapping.source_view.external_id, mapping.destination_view.external_id)
            for mapping in mappings
            if "bservation" in mapping.source_view.external_id or "bservation" in mapping.destination_view.external_id
        ] == []

    def test_custom_observation_view_replaces_field_observation(self) -> None:
        setup, _lookup = _setup(
            "Infield data",
            cdm_configs=[_cdm("loc", "cdm_app", view_mappings=_observation_mappings(CUSTOM_OBSERVATION_VIEW))],
        )

        mappings = setup.infield_mappings(InfieldMigrationSpaces(source="app_space", _target="cdm_app"))
        observation = next(mapping for mapping in mappings if mapping.source_view.external_id == "Observation")

        assert observation.destination_view == CUSTOM_OBSERVATION_VIEW

    def test_custom_observation_view_is_scoped_to_selected_target(self) -> None:
        setup, _lookup = _setup(
            "Infield data",
            cdm_configs=[
                _cdm("loc1", "cdm_app", view_mappings=_observation_mappings(CUSTOM_OBSERVATION_VIEW)),
                _cdm("loc2", "other_app", view_mappings=_observation_mappings(OTHER_OBSERVATION_VIEW)),
            ],
        )
        mappings = setup.infield_mappings(InfieldMigrationSpaces(source="app_space", _target="cdm_app"))
        observation = next(mapping for mapping in mappings if mapping.source_view.external_id == "Observation")

        assert observation.destination_view == CUSTOM_OBSERVATION_VIEW

    def test_conflicting_observation_views_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_targets(monkeypatch, {"ASSET_1": "cdm_a", "ASSET_2": "cdm_b"})
        setup, lookup = _setup(
            "Infield data",
            cdm_configs=[
                _cdm("loc1", "cdm_a", view_mappings=_observation_mappings(CUSTOM_OBSERVATION_VIEW)),
                _cdm("loc2", "cdm_b", view_mappings=_observation_mappings(OTHER_OBSERVATION_VIEW)),
            ],
        )
        lookup.source_space = "shared_app"

        with pytest.raises(ToolkitMigrationError, match="disagree on the custom observation view"):
            setup.infield_mappings(InfieldMigrationSpaces(source="shared_app", _target=None))

    def test_infield_selectors_follow_legacy_rules(self) -> None:
        source = "app_space"
        mappings = create_infield_data_mappings()
        selectors = InFieldSetup.get_infield_data_selectors(
            InfieldMigrationSpaces(source=source, _target="cdm_app"), mappings
        )
        schedule_selector = create_infield_schedule_selector(instance_space=source)

        summary: list[tuple[str, object]] = []
        for mapping, selector in zip(mappings, selectors, strict=True):
            if mapping.source_view.external_id == "Schedule":
                summary.append(("Schedule", selector == schedule_selector))
                continue
            if not isinstance(selector, InstanceViewSelector):
                summary.append((mapping.source_view.external_id, type(selector).__name__))
                continue
            edge_types = tuple(dict.fromkeys(mapping.edge_mapping or {})) or None
            summary.append(
                (
                    mapping.source_view.external_id,
                    (
                        selector.endpoint,
                        selector.instance_spaces,
                        selector.edge_types,
                        selector.edge_types == edge_types,
                    ),
                )
            )

        expected: list[tuple[str, object]] = []
        for mapping in mappings:
            if mapping.source_view.external_id == "Schedule":
                expected.append(("Schedule", True))
                continue
            expected.append((mapping.source_view.external_id, ("sync", (source,), _selector_edge_types(mapping), True)))

        assert summary == expected

    def test_source_selectors_use_mapping_views(self) -> None:
        setup, lookup = _setup(
            "APM_SourceData",
            apm_configs=[
                _apm(
                    _root("loc", "ASSET_1", "app_space", "source_space"),
                    view_mappings={"activity": _source_view_mapping(CUSTOM_ACTIVITY_VIEW)},
                )
            ],
            cdm_configs=[_cdm("loc", "cdm_app", SOURCE_DATA_FILTERS)],
        )
        spaces = InfieldMigrationSpaces(source="source_space", _target="cdm_source")
        mappings = setup.create_source_mappings(spaces, resolve_apm_source_data_view_ids(lookup.apm_configs))
        selectors = InFieldSetup.get_infield_source_selectors(spaces, mappings)

        activity = next(
            mapping for mapping in mappings if mapping.external_id == "APMActivityToMaintenanceOrderMapping"
        )
        activity_selector = next(
            selector
            for selector in selectors
            if isinstance(selector, InstanceViewSelector)
            and selector.view.external_id == activity.source_view.external_id
        )

        assert (
            activity.source_view,
            activity_selector.view.space,
            activity_selector.view.version,
            activity_selector.instance_spaces,
            activity_selector.endpoint,
        ) == (CUSTOM_ACTIVITY_VIEW, CUSTOM_ACTIVITY_VIEW.space, CUSTOM_ACTIVITY_VIEW.version, ("source_space",), "sync")

    def test_custom_destination_views_are_remapped(self) -> None:
        setup, lookup = _setup(
            "APM_SourceData",
            cdm_configs=[
                _cdm(
                    "loc",
                    "cdm_app",
                    SOURCE_DATA_FILTERS,
                    {"operation": _source_view_mapping(CUSTOM_OPERATION_VIEW)},
                )
            ],
        )
        mappings = setup.create_source_mappings(
            InfieldMigrationSpaces(source="source_space", _target="cdm_source"),
            resolve_apm_source_data_view_ids(lookup.apm_configs),
        )
        operation = next(mapping for mapping in mappings if mapping.source_view.external_id == "APM_Operation")

        assert operation.destination_view == CUSTOM_OPERATION_VIEW

    def test_conflicting_views_in_one_target_fall_back_to_default(self) -> None:
        setup, lookup = _setup(
            "APM_SourceData",
            cdm_configs=[
                _cdm(
                    "loc1", "cdm_app", SOURCE_DATA_FILTERS, {"operation": _source_view_mapping(CUSTOM_OPERATION_VIEW)}
                ),
                _cdm("loc2", "cdm_app", SOURCE_DATA_FILTERS, {"operation": _source_view_mapping(OTHER_OPERATION_VIEW)}),
            ],
        )
        mappings = setup.create_source_mappings(
            InfieldMigrationSpaces(source="source_space", _target="cdm_source"),
            resolve_apm_source_data_view_ids(lookup.apm_configs),
        )
        operation = next(mapping for mapping in mappings if mapping.source_view.external_id == "APM_Operation")

        console_print = cast(MagicMock, lookup.client.console.print)
        assert (operation.destination_view.external_id, console_print.called) == ("CogniteOperation", True)

    def test_conflicting_destination_views_across_targets_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_targets(monkeypatch, {"ASSET_1": "cdm_a", "ASSET_2": "cdm_b"})
        setup, lookup = _setup(
            "APM_SourceData",
            cdm_configs=[
                _cdm(
                    "loc1",
                    "cdm_a",
                    {"operations": {"instanceSpaces": ["cdm_a"]}},
                    {"operation": _source_view_mapping(CUSTOM_OPERATION_VIEW)},
                ),
                _cdm(
                    "loc2",
                    "cdm_b",
                    {"operations": {"instanceSpaces": ["cdm_b"]}},
                    {"operation": _source_view_mapping(OTHER_OPERATION_VIEW)},
                ),
            ],
        )
        lookup.source_space = "shared_source"

        with pytest.raises(ToolkitMigrationError, match="disagree on the custom operation view"):
            setup.create_source_mappings(
                InfieldMigrationSpaces(source="shared_source", _target=None),
                resolve_apm_source_data_view_ids(lookup.apm_configs),
            )


def _selector_edge_types(mapping: ViewToViewMapping) -> tuple[object, ...] | None:
    return tuple(dict.fromkeys(mapping.edge_mapping or {})) or None


class TestInFieldMappers:
    @pytest.mark.parametrize(
        "operation, source_space, target_space, passthrough",
        [
            pytest.param("Infield data", "app_space", "cdm_app", True, id="infield_data"),
            pytest.param("APM_SourceData", "source_space", "cdm_source", False, id="apm_source_data"),
        ],
    )
    def test_non_split_instance_id_mapping_matches_legacy(
        self, operation: Operation, source_space: str, target_space: str, passthrough: bool
    ) -> None:
        setup, lookup = _setup(
            operation, [_apm(_root("loc", "ASSET_1", "app_space", "source_space"))], [_cdm("loc", "cdm_app")]
        )
        spaces = InfieldMigrationSpaces(source=source_space, _target=target_space)
        mapper = _mapper(setup, lookup, spaces)

        assert (
            type(mapper).__name__,
            _mapped_space(mapper, source_space),
            _mapped_space(mapper, "cognite_app_data"),
            mapper._connection_creator._direct_relation_edge_tiebreakers,
        ) == (
            "FDMtoCDMMapper",
            target_space,
            "cognite_app_data" if passthrough else None,
            DIRECT_RELATION_EDGE_TIEBREAKERS if operation == "Infield data" else {},
        )

    @pytest.mark.parametrize(
        "operation, passthrough, target_kind",
        [
            pytest.param("Infield data", True, "app_data", id="infield_data"),
            pytest.param("APM_SourceData", False, "source_data", id="apm_source_data"),
        ],
    )
    def test_location_split_instance_id_mapping_matches_legacy(
        self, operation: Operation, passthrough: bool, target_kind: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen = _patch_targets(monkeypatch)
        setup, lookup = _setup(operation, [_shared_apm()], [_cdm("loc1", "cdm_a"), _cdm("loc2", "cdm_b")])
        lookup.source_space = "legacy_space"
        spaces = InfieldMigrationSpaces(source="legacy_space", _target=None)
        mapper = _mapper(setup, lookup, spaces)

        assert (
            type(mapper).__name__,
            seen["target_kind"],
            getattr(mapper, "_source_views", None) is not None,
            _mapped_space(mapper, "cognite_app_data"),
        ) == (
            "LocationSplitFDMtoCDMMapper",
            target_kind,
            operation == "APM_SourceData",
            "cognite_app_data" if passthrough else None,
        )

    def test_missing_schedule_mapping_raises_legacy_error(self) -> None:
        setup, _lookup = _setup("Infield data")
        mappings = [
            mapping for mapping in create_infield_data_mappings() if mapping.source_view.external_id != "Schedule"
        ]

        try:
            setup.get_infield_data_mapper(InfieldMigrationSpaces(source="app_space", _target="cdm_app"), mappings)
        except ValueError as exc:
            assert "No mapping for Schedule view found in infield_data_mappings.yaml" in str(exc)
        except Exception as exc:
            pytest.fail(
                "Expected ValueError('No mapping for Schedule view found in infield_data_mappings.yaml'), "
                f"got {type(exc).__name__}: {exc}"
            )
        else:
            pytest.fail("Expected ValueError for a missing Schedule mapping")

    def test_missing_solution_tag_mapping_raises_legacy_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_targets(monkeypatch)
        setup, lookup = _setup("Infield data")
        lookup.source_space = "shared_app"
        mappings = [
            mapping for mapping in create_infield_data_mappings() if mapping.source_view != COGNITE_SOLUTION_TAG_VIEW_ID
        ]

        try:
            setup.get_infield_data_mapper(InfieldMigrationSpaces(source="shared_app", _target=None), mappings)
        except ValueError as exc:
            assert "No mapping for CogniteSolutionTag view found in infield_data_mappings.yaml" in str(exc)
        except Exception as exc:
            pytest.fail(
                "Expected ValueError('No mapping for CogniteSolutionTag view found in infield_data_mappings.yaml'), "
                f"got {type(exc).__name__}: {exc}"
            )
        else:
            pytest.fail("Expected ValueError for a missing CogniteSolutionTag mapping")


def _mapper(setup: InFieldSetup, lookup: InFieldLookup, spaces: InfieldMigrationSpaces) -> FDMtoCDMMapper:
    if lookup.operation == "Infield data":
        return setup.get_infield_data_mapper(spaces, setup.infield_mappings(spaces))
    source_views = resolve_apm_source_data_view_ids(lookup.apm_configs)
    return setup.get_infield_source_mapper(spaces, setup.create_source_mappings(spaces, source_views), source_views)


class TestMigrateAppWiring:
    @pytest.mark.parametrize(
        "command_name",
        [
            pytest.param("infield_data", id="infield_data"),
            pytest.param("infield_source_data", id="infield_source_data"),
        ],
    )
    def test_command_rejects_target_without_source(self, command_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
        _Prompts(["app_space", "cdm_app"]).install(monkeypatch)
        apm_configs, cdm_configs = _standard_infield_configs()
        client = _mock_client(apm_configs, cdm_configs)
        command: Callable[..., None] = getattr(MigrateApp, command_name)

        with (
            patch("cognite_toolkit._cdf_tk.apps._migrate_app._get_client", return_value=client),
            patch("cognite_toolkit._cdf_tk.apps._migrate_app.MigrationCommand.run", return_value=None),
            pytest.raises(
                typer.BadParameter,
                match="Either both --source-space and --target-space must be provided, or neither",
            ),
        ):
            command(MagicMock(), source_space=None, target_space="cdm_app")
