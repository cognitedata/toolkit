from cognite.client.data_classes.data_modeling import NodeId, Space
from scripts.regsetup import description
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteActivityApply
from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.batch_processor import HTTPBatchProcessor
class TestBatchProcessor:
    def test_batch_processor_create_nodes(self, toolkit_client_config: ToolkitClientConfig, toolkit_space: Space) -> None:
        config = toolkit_client_config
        url = f"https://{config.cdf_cluster}.cognitedata.com/api/v1/projects/{config.project}/models/instances"
        processor = HTTPBatchProcessor[NodeId](
            endpoint_url=url,
            config=config,
            as_id=lambda item: item.as_id(),
            method="POST",
            batch_size=1000,
            max_workers=2,
            description="Create nodes"
        )
        some_nodes = (
            CogniteActivityApply(
                space=toolkit_space.space,
                external_id=f"toolkit_test_batch_processor_{i}",
                name=f"Test Activity {i}",
            )
            for i in range(10)
        )

        result = processor.process(some_nodes)

        assert result.total_successful == 10
