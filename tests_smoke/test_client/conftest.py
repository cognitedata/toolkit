import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    DataModelRequest,
    SpaceRequest,
    ViewRequest,
    ContainerResponse,
    SpaceResponse,
    ContainerPropertyDefinition,
    TextProperty, ViewResponse, ViewCorePropertyRequest,
)
from tests_smoke.constants import SMOKE_SPACE, SMOKE_TEST_VIEW_EXTERNAL_ID, SMOKE_TEST_CONTAINER_EXTERNAL_ID


@pytest.fixture(scope="session")
def smoke_container(toolkit_client: ToolkitClient, smoke_space: SpaceResponse) -> ContainerResponse:
    return toolkit_client.tool.containers.create([
            ContainerRequest(
                space=SMOKE_SPACE,
                external_id=SMOKE_TEST_CONTAINER_EXTERNAL_ID,
                name="Toolkit Smoke Test Container",
                properties={
                    "name": ContainerPropertyDefinition(type=TextProperty())
                }
            )
        ]
    )

@pytest.fixture(scope="session")
def smoke_view(toolkit_client: ToolkitClient, smoke_space: SpaceResponse, smoke_container: ContainerResponse) -> ViewResponse:
    return toolkit_client.tool.views.create([
            ViewRequest(
                space=SMOKE_SPACE,
                external_id=SMOKE_TEST_VIEW_EXTERNAL_ID,
                version="v1",
                name="Toolkit Smoke Test View",
                properties={
                    "name": ViewCorePropertyRequest(
                        container=smoke_container.as_id(),
                        container_property_identifier="name"
                    )
                }
            )
        ]
    )
