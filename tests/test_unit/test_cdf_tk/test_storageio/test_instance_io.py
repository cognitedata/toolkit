import responses
from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.storageio import InstanceIO, InstanceViewSelector


class TestInstanceIO:
    def test_download_instance_ids(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = toolkit_config.create_api_url("/models/instances/list")
        selector = InstanceViewSelector(
            ViewId("mySpace", "myView", "v42"), instance_type="node", instance_spaces=("my_insta_space",)
        )
        N = 2500
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                toolkit_config.create_api_url("/models/instances/aggregate"),
                status=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "aggregates": [{"aggregate": "count", "property": "externalId", "value": N}],
                        }
                    ]
                },
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={
                    "items": [
                        {
                            "externalId": f"instance_{i}",
                            "space": "my_space",
                            "instanceType": "node",
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "version": 1,
                        }
                        for i in range(N)
                    ]
                },
            )
            io = InstanceIO(client)
            ids = list(io.download_ids(selector))
            count = io.count(selector)
        assert len(list(ids)) == N // io.chunk_size + (1 if N % io.chunk_size > 0 else 0)
        total_ids = sum(len(chunk) for chunk in ids)
        assert total_ids == N
        assert count == N
