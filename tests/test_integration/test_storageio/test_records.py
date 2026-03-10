import pytest
from cognite.client.data_classes.data_modeling import Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    TextProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.storageio import RecordIO
from cognite_toolkit._cdf_tk.storageio.selectors._records import (
    RecordContainerSelector,
    SelectedContainer,
    SelectedStream,
)
from tests.test_integration.constants import RECORD_COUNT


@pytest.fixture(scope="module")
def record_container(toolkit_client: ToolkitClient, toolkit_space: Space) -> ContainerResponse:
    """Record container with a name property, created if it doesn't exist."""
    container = ContainerRequest(
        space=toolkit_space.space,
        external_id="toolkit_test_record_container",
        name="Test Record Container",
        used_for="record",
        properties={
            "name": ContainerPropertyDefinition(type=TextProperty()),
        },
    )
    retrieved = toolkit_client.tool.containers.retrieve([container.as_id()])
    if retrieved:
        return retrieved[0]
    created = toolkit_client.tool.containers.create([container])
    assert created, "Failed to create record container"
    return created[0]


@pytest.fixture(scope="module")
def record_selector(
    toolkit_client: ToolkitClient, record_container: ContainerResponse, toolkit_stream: StreamResponse
) -> RecordContainerSelector:
    """Ensure records exist in the stream via upsert and return a selector."""
    selector = RecordContainerSelector(
        stream=SelectedStream(external_id=toolkit_stream.external_id),
        container=SelectedContainer(space=record_container.space, external_id=record_container.external_id),
        initialize_cursor="9999d-ago",
        download_dir_name=None,
        instance_spaces=(record_container.space,),
    )

    items = [
        {
            "space": record_container.space,
            "externalId": f"toolkit_test_record_{i}",
            "sources": [
                {
                    "source": {
                        "type": "container",
                        "space": record_container.space,
                        "externalId": record_container.external_id,
                    },
                    "properties": {"name": f"Test Record {i}"},
                }
            ],
        }
        for i in range(RECORD_COUNT)
    ]
    upsert_url = f"/streams/{toolkit_stream.external_id}/records/upsert"
    with HTTPClient(toolkit_client.config) as http_client:
        result = http_client.request_single_retries(
            RequestMessage(
                endpoint_url=toolkit_client.config.create_api_url(upsert_url),
                method="POST",
                body_content={"items": items},
            )
        )
        result.get_success_or_raise()

    return selector


class TestRecordIO:
    def test_stream_records(self, toolkit_client: ToolkitClient, record_selector: RecordContainerSelector) -> None:
        io = RecordIO(toolkit_client)
        pages = list(io.stream_data(record_selector))

        results = [record for page in pages for record in page.items]
        assert len(results) >= RECORD_COUNT, f"Expected at least {RECORD_COUNT} records, got {len(results)}"
