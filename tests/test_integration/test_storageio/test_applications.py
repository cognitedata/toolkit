from typing import Any

import pytest
from cognite.client.data_classes import DataSet, Event, EventWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.storageio import CanvasIO
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, SuccessResponse


@pytest.fixture
def canvas_event(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> Event:
    event = EventWrite(
        external_id="canvas_event_integration_test",
        start_time=1_600_000_000_000,
        end_time=1_600_000_360_000,
        data_set_id=toolkit_dataset.id,
        description="Integration test event for canvas",
        type="canvas_test_event",
        subtype="canvas_test_event",
    )
    if existing := toolkit_client.events.retrieve(external_id=event.external_id):
        return existing
    created_event = toolkit_client.events.create(event)
    return created_event


@pytest.fixture
def canvas_raw_data(canvas_event: Event) -> dict[str, Any]:
    user_id = "ndTFZh9K9-m2W9WBKc30-Q"
    canvas_id = "70c66b27-c16e-4f6c-bc1e-940f086ec2f7"
    event_ref = "167ab784-9f33-4130-831e-91d820359ce9"
    return {
        "canvas": {
            "externalId": canvas_id,
            "instanceType": "node",
            "space": "IndustrialCanvasInstanceSpace",
            "sources": [
                {
                    "source": {
                        "space": "cdf_industrial_canvas",
                        "externalId": "Canvas",
                        "version": "v7",
                        "type": "view",
                    },
                    "properties": {
                        "createdBy": user_id,
                        "isLocked": None,
                        "visibility": "public",
                        "sourceCanvasId": None,
                        "isArchived": None,
                        "context": [{"type": "FILTERS", "payload": {"filters": []}}],
                        "name": "Toolkit Integration Test Canvas",
                        "solutionTags": None,
                        "updatedAt": "2025-11-23T08:09:13.183+00:00",
                        "updatedBy": user_id,
                    },
                }
            ],
        },
        "containerReferences": [
            {
                "externalId": f"{canvas_id}_{event_ref}",
                "instanceType": "node",
                "space": "IndustrialCanvasInstanceSpace",
                "sources": [
                    {
                        "source": {
                            "space": "cdf_industrial_canvas",
                            "externalId": "ContainerReference",
                            "version": "v2",
                            "type": "view",
                        },
                        "properties": {
                            "chartsId": None,
                            "containerReferenceType": "event",
                            "height": 500,
                            "id": event_ref,
                            "label": "canvas_test_event: canvas_event_integration_test",
                            "properties": {"zIndex": 0, "unscaledWidth": 600, "unscaledHeight": 500},
                            "maxHeight": None,
                            "maxWidth": None,
                            "resourceExternalId": canvas_event.external_id,
                            "resourceSubId": None,
                            "width": 600,
                            "x": -10,
                            "y": 418,
                        },
                    }
                ],
            },
        ],
    }


class TestCanvasIO:
    def test_upload_canvas(self, toolkit_client: ToolkitClient, canvas_raw_data: dict[str, Any]) -> None:
        io = CanvasIO(toolkit_client)
        upload_items = io.json_chunk_to_data([("canvas_data.json", canvas_raw_data)])

        assert len(upload_items) == 1

        with HTTPClient(toolkit_client.config) as http_client:
            upload_result = io.upload_items(upload_items, http_client)

        not_success = [message for message in upload_result if not isinstance(message, SuccessResponse)]
        assert len(not_success) == 0, f"Some canvas items failed to upload: {humanize_collection(not_success)}"
