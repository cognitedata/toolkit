import json
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import responses
import respx
from cognite.client.data_classes import Asset, AssetList
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DataModel,
    DataModelList,
    DirectRelation,
    MappedProperty,
    NodeApply,
    NodeOrEdgeData,
    Text,
    View,
    ViewId,
)
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, ProjectStatistics

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import (
    COGNITE_MIGRATION_MODEL,
    INSTANCE_SOURCE_VIEW_ID,
    MODEL_ID,
    RESOURCE_VIEW_MAPPING_VIEW_ID,
)
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import _ASSET_ID, create_default_mappings
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import AssetIO, InstanceIO


@pytest.fixture
def cognite_migration_model(
    toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock
) -> Iterator[responses.RequestsMock]:
    config = toolkit_config
    mapping_by_id = {mapping.external_id: mapping for mapping in create_default_mappings()}
    asset_mapping = mapping_by_id[_ASSET_ID]
    # Lookup of the mapping in the Migration Model
    mapping_node_response = asset_mapping.dump(context="api")
    mapping_node_response.update({"createdTime": 0, "lastUpdatedTime": 0, "version": 1})
    sources = mapping_node_response.pop("sources", [])
    if sources:
        mapping_view_id = asset_mapping.sources[0].source
        mapping_node_response["properties"] = {
            mapping_view_id.space: {
                f"{mapping_view_id.external_id}/{mapping_view_id.version}": sources[0]["properties"]
            }
        }
    rsps.post(
        config.create_api_url("models/instances/byids"),
        json={"items": [mapping_node_response]},
        status=200,
    )

    # Lookup CogniteAsset, this is not the full model, just the properties we need for the
    # migration
    default_prop_args = dict(nullable=True, immutable=False, auto_increment=False)
    default_view_args = dict(
        last_updated_time=1,
        created_time=1,
        description=None,
        name=None,
        implements=[],
        writable=True,
        used_for="node",
        is_global=True,
        filter=None,
    )
    cognite_asset = View(
        space="cdf_cdm",
        external_id="CogniteAsset",
        version="v1",
        properties={
            "name": MappedProperty(ContainerId("cdf_cdm", "CogniteDescribable"), "name", Text(), **default_prop_args),
            "description": MappedProperty(
                ContainerId("cdf_cdm", "CogniteDescribable"), "description", Text(), **default_prop_args
            ),
            "source": MappedProperty(
                ContainerId("cdf_cdm", "CogniteSourceable"), "name", DirectRelation(), **default_prop_args
            ),
            "tags": MappedProperty(
                ContainerId("cdf_cdm", "CogniteDescribable"), "tags", Text(is_list=True), **default_prop_args
            ),
        },
        **default_view_args,
    )
    rsps.post(
        config.create_api_url("models/views/byids"),
        json={"items": [cognite_asset.dump()]},
    )
    # Migration model
    migration_model = COGNITE_MIGRATION_MODEL.dump()
    migration_model["createdTime"] = 1
    migration_model["lastUpdatedTime"] = 1
    migration_model["isGlobal"] = False
    rsps.post(config.create_api_url("models/datamodels/byids"), json={"items": migration_model})

    yield rsps


class TestMigrationCommand:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_migrate_assets(
        self,
        toolkit_config: ToolkitClientConfig,
        cognite_migration_model: responses.RequestsMock,
        tmp_path: Path,
        respx_mock: respx.MockRouter,
    ) -> None:
        rsps = cognite_migration_model
        config = toolkit_config
        assets = AssetList(
            [
                Asset(
                    id=1000 + i,
                    external_id=f"asset_{i}",
                    name=f"Asset {i}",
                    description=f"This is Asset {i}",
                    last_updated_time=1,
                    created_time=0,
                    parent_external_id="asset_0" if i > 0 else None,
                )
                for i in range(2)
            ]
        )
        space = "my_space"
        csv_content = "id,space,externalId,ingestionView\n" + "\n".join(
            f"{1000 + i},{space},asset_{i},{_ASSET_ID}" for i in range(len(assets))
        )

        # Asset retrieve ids
        rsps.post(
            config.create_api_url("/assets/byids"),
            json={"items": [asset.dump() for asset in assets]},
            status=200,
        )
        # Instance creation
        respx.post(
            config.create_api_url("/models/instances"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "space": space,
                            "externalId": f"asset_{i}",
                            "version": 1,
                            "wasModified": True,
                            "createdTime": 1,
                            "lastUpdatedTime": 1,
                        }
                        for i in range(len(assets))
                    ]
                },
            )
        )

        csv_file = tmp_path / "migration.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)

        result = command.migrate(
            selected=MigrationCSVFileSelector(csv_file, resource_type="asset"),
            data=AssetCentricMigrationIOAdapter(client, AssetIO(client), InstanceIO(client)),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )

        # Check that the assets were uploaded
        last_call = respx_mock.calls[-1]
        assert last_call.request.url == config.create_api_url("/models/instances")
        assert last_call.request.method == "POST"
        actual_instances = json.loads(last_call.request.content)["items"]
        expected_instance = [
            NodeApply(
                space=space,
                external_id=asset.external_id,
                sources=[
                    NodeOrEdgeData(
                        source=ViewId("cdf_cdm", "CogniteAsset", "v1"),
                        properties={
                            "name": asset.name,
                            "description": asset.description,
                        },
                    ),
                    NodeOrEdgeData(
                        source=INSTANCE_SOURCE_VIEW_ID,
                        properties={
                            "id": asset.id,
                            "resourceType": "asset",
                            "dataSetId": None,
                            "classicExternalId": asset.external_id,
                        },
                    ),
                ],
            ).dump()
            for asset in assets
        ]
        assert actual_instances == expected_instance
        actual_results = [result.get_progress(asset.id) for asset in assets]
        expected_results = [{"download": "success", "convert": "success", "upload": "success"} for _ in assets]
        assert actual_results == expected_results

    def test_validate_migration_model_available(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.data_models.retrieve.return_value = DataModelList([])
            with pytest.raises(ToolkitMigrationError):
                MigrationCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_multiple_models(self) -> None:
        """Test that multiple models raises an error."""
        with monkeypatch_toolkit_client() as client:
            # Create mock models with the expected MODEL_ID
            model1 = MagicMock(spec=DataModel)
            model1.as_id.return_value = MODEL_ID
            model2 = MagicMock(spec=DataModel)
            model2.as_id.return_value = MODEL_ID

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model1, model2])

            with pytest.raises(ToolkitMigrationError) as exc_info:
                MigrationCommand.validate_migration_model_available(client)

            assert "Multiple migration models" in str(exc_info.value)

    def test_validate_migration_model_available_missing_views(self) -> None:
        """Test that a model with missing views raises an error."""
        with monkeypatch_toolkit_client() as client:
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has views but missing the required ones
            model.views = [INSTANCE_SOURCE_VIEW_ID]  # Missing VIEW_SOURCE_VIEW_ID

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model])

            with pytest.raises(ToolkitMigrationError, match=r"Invalid migration model. Missing views"):
                MigrationCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_success(self) -> None:
        """Test that a valid model with all required views succeeds."""
        with monkeypatch_toolkit_client() as client:
            # Mocking the migration Model to get a response format of the model.
            # An alternative would be to write a conversion of write -> read format of the model
            # which is a significant amount of logic.
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has all required views
            model.views = [INSTANCE_SOURCE_VIEW_ID, RESOURCE_VIEW_MAPPING_VIEW_ID]

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model])

            # Should not raise any exception
            MigrationCommand.validate_migration_model_available(client)

            client.data_modeling.data_models.retrieve.assert_called_once_with([MODEL_ID], inline_views=False)

    def test_validate_available_capacity_missing_capacity(self) -> None:
        cmd = MigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=1500,
                soft_deleted_instances_limit=10_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            with pytest.raises(ToolkitValueError) as exc_info:
                cmd.validate_available_capacity(client, 10_000)

        assert "Cannot proceed with migration" in str(exc_info.value)

    def test_validate_available_capacity_sufficient_capacity(self) -> None:
        cmd = MigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=5_000_000,
                soft_deleted_instances_limit=100_000_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            cmd.validate_available_capacity(client, 10_000)
