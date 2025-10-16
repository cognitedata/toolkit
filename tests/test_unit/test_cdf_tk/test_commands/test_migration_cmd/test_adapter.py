from pathlib import Path

import pytest
import responses
from cognite.client.data_classes.aggregations import UniqueResult, UniqueResultList
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeList, NodeOrEdgeData, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    AssetCentricMigrationIOAdapter,
    MigrationCSVFileSelector,
    SourceSystemCreation,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_model import CREATED_SOURCE_SYSTEM_VIEW_ID
from cognite_toolkit._cdf_tk.storageio import AssetIO
from cognite_toolkit._cdf_tk.storageio.selectors import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector


class TestAssetCentricMigrationIOAdapter:
    def test_download(self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, tmp_path: Path) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config)
        N = 1500
        items = [{"id": i, "externalId": f"asset_{i}", "space": "mySpace"} for i in range(N)]
        rsps.post(config.create_api_url("/assets/byids"), json={"items": items[: AssetIO.CHUNK_SIZE]})
        rsps.post(config.create_api_url("/assets/byids"), json={"items": items[AssetIO.CHUNK_SIZE :]})

        csv_file = tmp_path / "files.csv"
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,asset_{i}" for i in range(N)))
        selector = MigrationCSVFileSelector(datafile=csv_file, resource_type="asset")
        adapter = AssetCentricMigrationIOAdapter(client, AssetIO(client))
        downloaded = list(adapter.stream_data(selector))
        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        unexpected_space = [
            item for chunk in downloaded for item in chunk if item.mapping.instance_id.space != "mySpace"
        ]
        assert not unexpected_space, f"Found items with unexpected space: {unexpected_space}"
        first_item = downloaded[0][0]
        assert first_item.dump() == {
            "mapping": {"id": 0, "instanceId": {"space": "mySpace", "externalId": "asset_0"}, "resourceType": "asset"},
            "resource": {"id": 0, "externalId": "asset_0"},
        }


class TestSourceSystemCreation:
    @pytest.mark.parametrize(
        "selector",
        [
            pytest.param(DataSetSelector(data_set_external_id="myDataSet", resource_type="asset"), id="dataset"),
            pytest.param(AssetSubtreeSelector(hierarchy="rootAsset", resource_type="asset"), id="subtree"),
        ],
    )
    def test_count_and_stream_data(self, selector: AssetCentricSelector) -> None:
        asset_sources = UniqueResultList([UniqueResult(100, ["aveva"]), UniqueResult(50, ["custom"])])
        event_sources = UniqueResultList(
            [UniqueResult(400, ["sap"]), UniqueResult(200, ["internal"])],
        )
        file_sources = UniqueResultList([UniqueResult(1000, ["sharepoint"])])
        space = "my_sources"
        expected_nodes = [
            NodeApply(
                space=space,
                external_id=source,
                sources=[
                    NodeOrEdgeData(
                        source=ViewId(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                        properties={"name": source},
                    ),
                    NodeOrEdgeData(source=CREATED_SOURCE_SYSTEM_VIEW_ID, properties={"source": source}),
                ],
            )
            for source in ("aveva", "custom", "sap", "internal", "sharepoint")
        ]
        with monkeypatch_toolkit_client() as client:
            client.assets.aggregate_cardinality_values.return_value = len(asset_sources)
            client.events.aggregate_cardinality_values.return_value = len(event_sources)
            client.documents.aggregate_cardinality_values.return_value = len(file_sources)
            client.assets.aggregate_unique_values.return_value = asset_sources
            client.events.aggregate_unique_values.return_value = event_sources
            client.documents.aggregate_unique_values.return_value = file_sources

            adapter = SourceSystemCreation(client, instance_space="my_sources")

            count = adapter.count(selector)
            assert count == len(expected_nodes)
            source_systems = NodeList[Node]([])
            for chunk in adapter.stream_data(selector):
                source_systems.extend(chunk)

            roundtrip = adapter.json_chunk_to_data(adapter.data_to_json_chunk(source_systems))
            assert [node.dump() for node in roundtrip] == [node.dump() for node in expected_nodes]
