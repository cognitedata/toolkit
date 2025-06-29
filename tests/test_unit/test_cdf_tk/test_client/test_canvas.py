from datetime import datetime
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client.api.canvas import CanvasAPI
from cognite_toolkit._cdf_tk.client.api.extended_data_modeling import ExtendedInstancesAPI
from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    Canvas,
    CanvasAnnotation,
    CanvasAnnotationApply,
    CanvasApply,
    CogniteSolutionTag,
    CogniteSolutionTagApply,
    ContainerReference,
    ContainerReferenceApply,
    FdmInstanceContainerReference,
    FdmInstanceContainerReferenceApply,
    IndustrialCanvasApply,
)
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestLoadDump:
    @pytest.mark.parametrize(
        "node_cls",
        [
            Canvas,
            CanvasApply,
            CanvasAnnotation,
            CanvasAnnotationApply,
            CogniteSolutionTag,
            CogniteSolutionTagApply,
            FdmInstanceContainerReference,
            FdmInstanceContainerReferenceApply,
            ContainerReference,
            ContainerReferenceApply,
        ],
    )
    def test_dump_reload(self, node_cls: type[CogniteResource]) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(node_cls)
        dumped = instance.dump()
        reloaded = node_cls.load(dumped)

        assert reloaded == instance, f"Expected: {instance}, but got: {reloaded}"


@pytest.fixture()
def instance_api() -> MagicMock:
    return MagicMock(spec=ExtendedInstancesAPI)


class TestCanvasAPI:
    def test_upsert(self, instance_api: MagicMock) -> None:
        canvas_api = CanvasAPI(instance_api=instance_api)

        canvas_apply = CanvasApply(
            external_id="test_canvas", name="Test Canvas", created_by="me", updated_by="me", updated_at=datetime.now()
        )
        canvas_api.upsert(canvas_apply)

        assert instance_api.apply.called
        assert instance_api.apply.call_args[0][0] == canvas_apply

    @pytest.mark.parametrize(
        "external_id, expected_arg",
        [
            ("test_canvas", [NodeId(CANVAS_INSTANCE_SPACE, "test_canvas")]),
            (
                ["canvas1", "canvas2"],
                [NodeId(CANVAS_INSTANCE_SPACE, "canvas1"), NodeId(CANVAS_INSTANCE_SPACE, "canvas2")],
            ),
        ],
    )
    def test_delete(
        self, external_id: str | list[str], expected_arg: NodeId | list[NodeId], instance_api: MagicMock
    ) -> None:
        canvas_api = CanvasAPI(instance_api=instance_api)

        canvas_api.delete(external_id)

        assert instance_api.delete.called
        assert instance_api.delete.call_args[0][0] == expected_arg

    @pytest.mark.parametrize(
        "external_id, expected_arg",
        [
            ("test_canvas", NodeId(CANVAS_INSTANCE_SPACE, "test_canvas")),
            (
                ["canvas1", "canvas2"],
                [NodeId(CANVAS_INSTANCE_SPACE, "canvas1"), NodeId(CANVAS_INSTANCE_SPACE, "canvas2")],
            ),
        ],
    )
    def test_retrieve(self, external_id: str | list[str], expected_arg: NodeId, instance_api: MagicMock) -> None:
        canvas_api = CanvasAPI(instance_api=instance_api)

        canvas_api.retrieve(external_id)

        assert instance_api.retrieve_nodes.called
        assert instance_api.retrieve_nodes.call_args[0][0] == expected_arg

    def test_list(self, instance_api: MagicMock) -> None:
        canvas_api = CanvasAPI(instance_api=instance_api)

        canvas_api.list(limit=100)

        assert instance_api.list.called
        assert instance_api.list.call_args[1] == {
            "instance_type": Canvas,
            "space": CANVAS_INSTANCE_SPACE,
            "limit": 100,
            "sort": None,
            "filter": None,
        }


class TestIndustrialCanvasDataClass:
    def test_create_backup(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvasApply)

        backup = instance.create_backup()

        original_ids = set(instance.as_instance_ids(include_solution_tags=False))
        backup_ids = set(backup.as_instance_ids(include_solution_tags=False))
        overlapping_ids = original_ids.intersection(backup_ids)
        assert not overlapping_ids

        original_ids = set(instance.as_instance_ids(include_solution_tags=True))
        backup_ids = set(backup.as_instance_ids(include_solution_tags=True))
        overlapping_ids = original_ids.intersection(backup_ids)
        solution_tags_ids = {tag.as_id() for tag in instance.solution_tags}
        assert solution_tags_ids == overlapping_ids, "Expected overlapping IDs to be solution tags IDs only"
