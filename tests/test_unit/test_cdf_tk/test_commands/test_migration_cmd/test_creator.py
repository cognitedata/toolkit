from pathlib import Path
from typing import Any

import pytest
import yaml
from cognite.client.data_classes.aggregations import UniqueResult, UniqueResultList
from cognite.client.data_classes.data_modeling import NodeList
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DataModelResponse,
    NodeReference,
    NodeRequest,
    SpaceRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import CreatedSourceSystem
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import (
    InfieldV2ConfigCreator,
    InstanceSpaceCreator,
    SourceSystemCreator,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, VIEWS
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitRequiredValueError
from tests.data import MIGRATION_DIR
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestCreator:
    @pytest.mark.parametrize("dry_run", [pytest.param(True, id="dry_run"), pytest.param(False, id="not_dry_run")])
    def test_create_instance_spaces(
        self, dry_run: bool, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModelResponse, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(ViewResponse, VIEWS)
        data_sets = [
            DataSetResponse(
                id=i,
                external_id=f"dataset_{letter}",
                name=f"Dataset {letter}",
                description=f"This is dataset {letter}",
                created_time=0,
                last_updated_time=0,
            )
            for i, letter in enumerate("ABC", 1)
        ]
        toolkit_client_approval.append(DataSetResponse, data_sets)

        _ = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InstanceSpaceCreator(
                toolkit_client_approval.client, data_set_external_ids=[ds.external_id for ds in data_sets]
            ),
            dry_run=dry_run,
            verbose=False,
            output_dir=tmp_path,
        )
        configurations = list(tmp_path.rglob("*Space.yaml"))
        assert len(configurations) == 3
        created_spaces = toolkit_client_approval.created_resources["SpaceResponse"] if not dry_run else []
        assert all(isinstance(space, SpaceRequest) for space in created_spaces)
        expected_created = {ds.external_id for ds in data_sets} if not dry_run else set()
        assert {space.space for space in created_spaces} == expected_created

    def test_create_instance_spaces_missing_external_id(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModelResponse, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(ViewResponse, VIEWS)
        data_sets = [
            DataSetResponse(
                id=i,
                name=f"Dataset {i}",
                description=f"This is dataset {i}",
                created_time=0,
                last_updated_time=0,
            )
            for i in range(3)
        ]

        with pytest.raises(
            ToolkitRequiredValueError,
            match="Cannot create instance spaces for datasets with missing external IDs: 0, 1 and 2",
        ):
            MigrationCommand(silent=True).create(
                client=toolkit_client_approval.client,
                creator=InstanceSpaceCreator(toolkit_client_approval.client, datasets=data_sets),
                dry_run=False,
                verbose=False,
                output_dir=tmp_path,
            )

    @pytest.mark.parametrize(
        "auto_fix", [pytest.param(True, id="auto_fix_enabled"), pytest.param(False, id="auto_fix_disabled")]
    )
    def test_create_instance_space_auto_fix(
        self, auto_fix: bool, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModelResponse, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(ViewResponse, VIEWS)

        invalid_dataset = DataSetResponse(
            id=1,
            external_id="invalid dataset name with spaces",
            name="Invalid Dataset",
            description="This dataset has an invalid external ID for a space.",
            created_time=0,
            last_updated_time=0,
        )

        creator = InstanceSpaceCreator(toolkit_client_approval.client, [invalid_dataset], auto_fix=auto_fix)
        created = list(creator.create_resources())
        assert len(created) == 1
        created_space = created[0].resources[0].resource
        assert isinstance(created_space, SpaceRequest)
        if auto_fix:
            assert created_space.space == "invaliddatasetnamewithspaces"
        else:
            assert created_space.space == "invalid dataset name with spaces"

    def test_create_instance_spaces_duplicate_space_ids(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModelResponse, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(ViewResponse, VIEWS)

        datasets = [
            DataSetResponse(
                id=1,
                external_id="my.dataset",
                name="Dataset dot",
                description="Has a dot",
                created_time=0,
                last_updated_time=0,
            ),
            DataSetResponse(
                id=2,
                external_id="my dataset",
                name="Dataset space",
                description="Has a space",
                created_time=0,
                last_updated_time=0,
            ),
        ]

        creator = InstanceSpaceCreator(toolkit_client_approval.client, datasets, auto_fix=True)
        with pytest.raises(ToolkitMigrationError, match="Multiple dataset external IDs resolve to the same space ID"):
            list(creator.create_resources())

    @pytest.mark.parametrize(
        "arguments",
        [
            pytest.param({"data_set_external_id": "my_data_set"}, id="with_data_set"),
            pytest.param({"hierarchy": "my_root_asset"}, id="with_hierarchy"),
        ],
    )
    def test_create_source_systems(
        self, arguments: dict[str, Any], toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModelResponse, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(ViewResponse, VIEWS)
        asset_sources = UniqueResultList([UniqueResult(100, ["aveva"]), UniqueResult(50, ["custom"])])
        event_sources = UniqueResultList([UniqueResult(400, ["sap"]), UniqueResult(200, ["internal"])])
        file_sources = UniqueResultList([UniqueResult(1000, ["sharepoint"])])
        client = toolkit_client_approval.mock_client
        client.assets.aggregate_unique_values.return_value = asset_sources
        client.events.aggregate_unique_values.return_value = event_sources
        client.documents.aggregate_unique_values.return_value = file_sources
        client.migration.created_source_system.list.return_value = NodeList[CreatedSourceSystem](
            [
                CreatedSourceSystem(
                    space="my_other_space",
                    external_id="sap",
                    version=1,
                    last_updated_time=1,
                    created_time=1,
                    source="sap",
                )
            ]
        )

        _ = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=SourceSystemCreator(toolkit_client_approval.client, "my_source_space", **arguments),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )

        configurations = list(tmp_path.rglob("*Node.yaml"))
        assert len(configurations) == 4
        expected_external_ids = {"aveva", "custom", "internal", "sharepoint"}
        created_nodes = toolkit_client_approval.created_resources["InstanceDefinition"]
        assert all(isinstance(node, NodeRequest) for node in created_nodes)
        assert {node.external_id for node in created_nodes} == expected_external_ids

    def test_create_infield_config(self, data_regression: DataRegressionFixture, tmp_path: Path) -> None:
        apm_config_path = MIGRATION_DIR / "infield_config" / "default_infield_config_minimal.yaml"
        apm_config = APMConfigResponse.model_validate(yaml.safe_load(apm_config_path.read_text()))

        output: dict[str, Any] = {}
        with monkeypatch_toolkit_client() as client:
            asset_external_id = apm_config.feature_configuration.root_location_configurations[0].asset_external_id
            client.migration.lookup.assets.return_value = NodeReference(space="migrated", external_id=asset_external_id)
            creator = InfieldV2ConfigCreator(client, apm_configs=[apm_config])
            for to_create in creator.create_resources():
                for resource in to_create.resources:
                    output.setdefault(to_create.display_name, []).append(resource.config_data)

                    # Ensure that the config is valid according to the YAML schema
                    to_create.crud_cls.yaml_cls.model_validate(resource.config_data)

        data_regression.check(output)
