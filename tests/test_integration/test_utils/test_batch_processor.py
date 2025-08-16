from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteActivityApply

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.batch_processor import HTTPIterableProcessor


class TestBatchProcessor:
    def test_batch_processor_create_nodes(
        self, toolkit_client_config: ToolkitClientConfig, toolkit_space: Space
    ) -> None:
        config = toolkit_client_config
        some_nodes = (
            CogniteActivityApply(
                space=toolkit_space.space if i < 9 else "non_existent_space",
                external_id=f"toolkit_test_batch_processor_{i}",
                name=f"Test Activity {i}",
            ).dump()
            for i in range(10)
        )
        with HTTPIterableProcessor[NodeId](
            endpoint_url=config.create_api_url("/models/instances"),
            config=config,
            as_id=lambda item: NodeId(item["space"], item["externalId"]),
            body_parameters={"autoCreateDirectRelations": True},
            method="POST",
            batch_size=1000,
            max_workers=2,
            description="Create nodes",
        ) as processor:
            result = processor.process(some_nodes)

        assert result.total_successful == 9
        assert result.total_failed == 1
