from datetime import datetime, timezone

import pytest
from cognite.client.data_classes import EventList, EventWrite, EventWriteList
from cognite.client.data_classes.data_modeling import EdgeId, NodeId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CanvasAnnotationApply,
    CanvasApply,
    ContainerReferenceApply,
    IndustrialCanvasApply,
)


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


def create_canvas(three_events: EventList) -> IndustrialCanvasApply:
    return IndustrialCanvasApply(
        canvas=CanvasApply(
            external_id="efc2de9d-27a5-4a3b-9779-dff11c572610",
            name="ToolkitTestData",
            created_by="ndTFZh9K9-m2W9WBKc30-Q",
            updated_at=datetime(2025, 6, 28, 10, 24, 34, tzinfo=timezone.utc),
            updated_by="ndTFZh9K9-m2W9WBKc30-Q",
            visibility="private",
            context=[{"type": "FILTERS", "payload": {"filters": []}}],
        ),
        annotations=[
            CanvasAnnotationApply(
                external_id="efc2de9d-27a5-4a3b-9779-dff11c572610_4fba01d2-bcb7-4871-934f-0ab7e754bce9",
                id_="4fba01d2-bcb7-4871-934f-0ab7e754bce9",
                annotation_type="polylineAnnotation",
                container_id="08ed536d-3d77-4692-80b2-d4a9ec6a3aea",
                is_selectable=True,
                is_draggable=True,
                is_resizable=True,
                properties_={
                    "style": {
                        "fill": "rgba(164,178,252,0.9)",
                        "stroke": "rgb(66, 85, 187)",
                        "opacity": 1,
                        "strokeWidth": 12,
                        "shouldEnableStrokeScale": True,
                    },
                    "zIndex": 3,
                    "vertices": [
                        {"x": -0.16666666666666666, "y": -0.27297185430463594},
                        {"x": 0.005, "y": 0.022971854304635456},
                    ],
                    "endEndType": "none",
                    "startEndType": "none",
                },
            ),
        ],
        container_references=[
            ContainerReferenceApply(
                external_id="efc2de9d-27a5-4a3b-9779-dff11c572610_5befa8e5-cdcf-4292-a6f9-ed176f9fb73c",
                container_reference_type="event",
                resource_id=three_events[0].id,
                id_="5befa8e5-cdcf-4292-a6f9-ed176f9fb73c",
                resource_sub_id=None,
                charts_id=None,
                label=three_events[0].external_id,
                properties_={"zIndex": 0, "unscaledWidth": 600, "unscaledHeight": 500},
                x=-287,
                y=-303,
                width=600,
                height=500,
                max_width=None,
                max_height=None,
            )
        ],
    )


class TestIndustrialCanvasAPI:
    def test_retrieve_non_existing(self, toolkit_client: ToolkitClient) -> None:
        result = toolkit_client.canvas.industrial.retrieve("non-existing-canvas")
        assert result is None, "Expected None when retrieving a non-existing canvas"

    def test_create_update_retrieve_delete(self, toolkit_client: ToolkitClient, three_events: EventList) -> None:
        canvas = create_canvas(three_events)

        deleted: list[NodeId | EdgeId] | None = None
        try:
            created = toolkit_client.canvas.industrial.create(canvas)
            assert set(created.as_ids()) == set(canvas.as_instance_ids(include_solution_tags=True))

            # Remove annotation
            canvas.annotations = []
            updated = toolkit_client.canvas.industrial.update(canvas)
            assert set(updated.as_ids()) == set(canvas.as_instance_ids(include_solution_tags=True))

            retrieved = toolkit_client.canvas.industrial.retrieve(canvas.as_id())

            assert retrieved.as_write().dump(keep_existing_version=False) == canvas.dump(keep_existing_version=False)

            deleted = toolkit_client.canvas.industrial.delete(canvas)

            assert toolkit_client.canvas.retrieve(canvas.as_id()) is None
        finally:
            if deleted is None:
                toolkit_client.data_modeling.instances.delete_fast(canvas.as_instance_ids())
