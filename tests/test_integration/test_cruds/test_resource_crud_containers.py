from contextlib import suppress
from time import sleep

import pandas as pd
import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ContainerReference, ContainerRequest
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import NameId
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    ThreeDModelClassicRequest,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, TimeSeriesCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.three_d_model import ThreeDModelCRUD
from tests.test_integration.constants import RUN_UNIQUE_ID


@pytest.fixture(scope="session")
def integration_space(cognite_client: CogniteClient) -> dm.Space:
    space = dm.SpaceApply(
        space="integration_test_cdf_tk",
        name="CDF Toolkit Test Space",
        description="Space used for running integration test",
    )
    return cognite_client.data_modeling.spaces.apply(space)


class TestTimeSeriesLoader:
    def test_create_populate_count_drop_data(self, toolkit_client: CogniteClient) -> None:
        timeseries = TimeSeriesRequest(
            external_id=f"test_create_populate_count_drop_data{RUN_UNIQUE_ID}", is_string=False
        )
        datapoints = pd.DataFrame(
            [{"timestamp": 0, timeseries.external_id: 0}, {"timestamp": 1, timeseries.external_id: 1}]
        ).set_index("timestamp")
        datapoints.index = pd.to_datetime(datapoints.index, unit="s")
        loader = TimeSeriesCRUD(toolkit_client, None)
        ts_ids = [timeseries.as_id()]

        try:
            created = loader.create([timeseries])
            assert len(created) == 1

            assert loader.count(ts_ids) == 0
            toolkit_client.time_series.data.insert_dataframe(datapoints)

            assert loader.count(ts_ids) == 2

            loader.drop_data(ts_ids)

            assert loader.count(ts_ids) == 0

            assert loader.delete(ts_ids) == 1

            assert not loader.retrieve(ts_ids)
        finally:
            toolkit_client.time_series.delete(external_id=timeseries.external_id, ignore_unknown_ids=True)


@pytest.fixture(scope="function")
def node_container(cognite_client: CogniteClient, integration_space: dm.Space) -> dm.Container:
    container = dm.ContainerApply(
        space=integration_space.space,
        external_id="test_create_populate_count_drop_data",
        name="Test Container",
        description="Container used for running integration test",
        used_for="node",
        properties={"name": dm.ContainerProperty(dm.Text())},
    )
    return cognite_client.data_modeling.containers.apply(container)


@pytest.fixture(scope="function")
def edge_container(cognite_client: CogniteClient, integration_space: dm.Space) -> dm.Container:
    container = dm.ContainerApply(
        space=integration_space.space,
        external_id="test_create_populate_count_drop_data_edge",
        name="Test Container Edge",
        description="Container used for running integration test",
        used_for="edge",
        properties={"name": dm.ContainerProperty(dm.Text())},
    )
    return cognite_client.data_modeling.containers.apply(container)


class TestContainerLoader:
    # The DMS service is fairly unstable, so we need to rerun the tests if they fail.
    @pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError", "ToolkitAPIError"])
    def test_populate_count_drop_data_node_container(
        self, node_container: dm.Container, toolkit_client: ToolkitClient
    ) -> None:
        node = dm.NodeApply(
            space=node_container.space,
            external_id=f"test_create_populate_count_drop_data{RUN_UNIQUE_ID}",
            sources=[dm.NodeOrEdgeData(source=node_container.as_id(), properties={"name": "Anders"})],
        )
        container_id = [ContainerReference(space=node_container.space, external_id=node_container.external_id)]

        loader = ContainerCRUD(toolkit_client, None)

        try:
            assert loader.count(container_id) == 0

            toolkit_client.data_modeling.instances.apply(nodes=[node])

            assert loader.count(container_id) == 1

            loader.drop_data(container_id)
            assert loader.count(container_id) == 0

            write_container = ContainerRequest.model_validate(node_container.as_write().dump())
            write_container.description = "Updated description"
            updated = loader.update([write_container])
            assert len(updated) == 1
            if updated[0].description != write_container.description:
                # The API is not always consistent in returning the updated description,
                # so we need to retrieve the container to verify the update
                sleep(1)
                updated = loader.retrieve(container_id)
            assert updated[0].description == write_container.description
        finally:
            loader.drop_data(container_id)

    # The DMS service is fairly unstable, so we need to rerun the tests if they fail.
    @pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError", "ToolkitAPIError"])
    def test_populate_count_drop_data_edge_container(
        self, edge_container: dm.Container, toolkit_client: ToolkitClient
    ) -> None:
        space = edge_container.space
        nodes = dm.NodeApplyList(
            [
                dm.NodeApply(
                    space=space,
                    external_id=f"test_create_populate_count_drop_data:start{RUN_UNIQUE_ID}",
                    sources=None,
                ),
                dm.NodeApply(
                    space=space,
                    external_id=f"test_create_populate_count_drop_data:end{RUN_UNIQUE_ID}",
                    sources=None,
                ),
            ]
        )
        edge = dm.EdgeApply(
            space=space,
            external_id=f"test_populate_count_drop_data_edge_container{RUN_UNIQUE_ID}",
            type=dm.DirectRelationReference(space, "test_edge_type"),
            start_node=(nodes[0].space, nodes[0].external_id),
            end_node=(nodes[1].space, nodes[1].external_id),
            sources=[dm.NodeOrEdgeData(source=edge_container.as_id(), properties={"name": "Anders"})],
        )
        container_id = [ContainerReference(space=edge_container.space, external_id=edge_container.external_id)]

        loader = ContainerCRUD(toolkit_client, None)

        try:
            assert loader.count(container_id) == 0

            toolkit_client.data_modeling.instances.apply(edges=[edge], nodes=nodes)

            assert loader.count(container_id) == 1

            loader.drop_data(container_id)
            assert loader.count(container_id) == 0

            write_container = ContainerRequest.model_validate(edge_container.as_write().dump())
            write_container.description = "Updated description"
            updated = loader.update([write_container])
            assert len(updated) == 1
            if updated[0].description != write_container.description:
                # The API is not always consistent in returning the updated description,
                # so we need to retrieve the container to verify the update
                sleep(1)
                updated = loader.retrieve([write_container.as_id()])
            assert updated[0].description == write_container.description
        finally:
            toolkit_client.data_modeling.instances.delete(nodes=nodes.as_ids(), edges=edge.as_id())


class Test3DModelLoader:
    def test_create_delete_model(self, toolkit_client: ToolkitClient) -> None:
        model = ThreeDModelClassicRequest(
            name=f"tmp_test_create_delete_model_{RUN_UNIQUE_ID}",
            metadata={
                "description": "My description",
            },
        )

        loader = ThreeDModelCRUD(toolkit_client, None)

        missing = toolkit_client.iam.verify_capabilities(loader.get_required_capability(None, read_only=False))
        assert not missing, f"Missing capabilities: {missing}"

        created: list[ThreeDModelClassicResponse] | None = None
        try:
            created = loader.create([model])
            assert len(created) == 1

            # Serialize and deserialize the model to get a copy
            update = created[0].as_request_resource().model_copy(deep=True)
            update.metadata["new_key"] = "new_value"

            updated = loader.update([update])
            assert len(updated) == 1
            assert updated[0].metadata["new_key"] == "new_value"

            delete_count = loader.delete([NameId(name=model.name)])
            assert delete_count == 1
        finally:
            # Ensure that the model is deleted even if the test fails.
            if created is not None:
                with suppress(ToolkitAPIError):
                    toolkit_client.tool.three_d.models_classic.delete([created[0].as_id()])
