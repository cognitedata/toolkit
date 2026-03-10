import pytest
from cognite.client.data_classes.data_modeling import Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    TextProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordSource
from cognite_toolkit._cdf_tk.storageio import RecordIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors._records import (
    RecordContainerSelector,
    SelectedContainer,
    SelectedStream,
)

RECORD_COUNT = 5


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
def record_selector(toolkit_client: ToolkitClient, record_container: ContainerResponse) -> RecordContainerSelector:
    """Ensure records exist in the stream and return a selector for downloading them."""
    selector = RecordContainerSelector(
        stream=SelectedStream(external_id="my-stream2"),
        container=SelectedContainer(space=record_container.space, external_id=record_container.external_id),
        download_dir_name=None,
    )

    records = [
        RecordRequest(
            space=record_container.space,
            external_id=f"toolkit_test_record_{i}",
            sources=[
                RecordSource(
                    source=record_container.as_id(),
                    properties={"name": f"Test Record {i}"},
                )
            ],
        )
        for i in range(RECORD_COUNT)
    ]
    upload_items = [UploadItem(source_id=record.external_id, item=record) for record in records]
    io = RecordIO(toolkit_client)
    with HTTPClient(toolkit_client.config) as http_client:
        io.upload_items(upload_items, http_client, selector=selector)

    return selector


class TestRecordIO:
    def test_stream_records(self, toolkit_client: ToolkitClient, record_selector: RecordContainerSelector) -> None:
        io = RecordIO(toolkit_client)
        pages = list(io.stream_data(record_selector))

        results = [record for page in pages for record in page.items]
        assert len(results) == RECORD_COUNT, f"Expected {RECORD_COUNT} records, got {len(results)}"
