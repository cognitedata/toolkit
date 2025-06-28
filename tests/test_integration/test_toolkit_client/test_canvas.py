import pytest
from cognite.client.data_classes import EventList, EventWrite, EventWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient


@pytest.fixture(scope="session")
def three_events(toolkit_client: ToolkitClient) -> EventList:
    events = EventWriteList(
        [
            EventWrite(
                external_id=f"toolkit_test_canvas_event_{i}",
                type="CanvasEvent",
                description="This is a test event for the canvas.",
            )
            for i in range(3)
        ]
    )
    existing = toolkit_client.events.retrieve_multiple(external_ids=events.as_external_ids(), ignore_unknown_ids=True)
    if len(existing) == len(events):
        return existing
    existing_ids = existing.as_external_ids()
    created = toolkit_client.events.create([e for e in events if e.external_id not in existing_ids])
    existing.extend(created)
    return existing


class TestIndustrialCanvasAPI:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, three_events: EventList) -> None:
        assert True
