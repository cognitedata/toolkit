import itertools
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
    IndustrialCanvas,
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
        assert len(original_ids) == len(backup_ids)
        overlapping_ids = original_ids.intersection(backup_ids)
        assert not overlapping_ids

        original_ids = set(instance.as_instance_ids(include_solution_tags=True))
        backup_ids = set(backup.as_instance_ids(include_solution_tags=True))
        overlapping_ids = original_ids.intersection(backup_ids)
        solution_tags_ids = {tag.as_id() for tag in instance.solution_tags}
        assert solution_tags_ids == overlapping_ids, "Expected overlapping IDs to be solution tags IDs only"

        existing_version = [
            item.as_id()
            for item in itertools.chain(
                [backup.canvas],
                backup.annotations,
                backup.container_references,
                backup.fdm_instance_container_references,
                # Solutions tags are expected to have existing versions as these are
                # not created, but reused in the backup
            )
            if item.existing_version is not None
        ]
        assert not existing_version, (
            f"Expected no existing versions in backup annotations. Found: {len(existing_version)}"
        )

    def test_dump_load(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvas)
        dumped = instance.dump()
        reloaded = IndustrialCanvas.load(dumped)

        assert reloaded.dump() == instance.dump(), "Failed to reload IndustrialCanvas."

    def test_as_instances(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvasApply)

        node_or_edges = instance.as_instances()

        # 1 canvas + solution tags + 2 x (rest - one for node and one for edge connecting canvas to node)
        expected_instance_count = (
            1
            + len(instance.solution_tags)
            + 2
            * (
                len(instance.container_references)
                + len(instance.fdm_instance_container_references)
                + len(instance.annotations)
            )
        )
        assert len(node_or_edges) == expected_instance_count

        assert len(instance.solution_tags) > 0, "Expected solution tags to be present in the instance."
        assert len(instance.as_instance_ids(include_solution_tags=False)) == expected_instance_count - len(
            instance.solution_tags
        )
        assert len(instance.as_instance_ids(include_solution_tags=True)) == expected_instance_count
        assert instance.as_id() == instance.canvas.external_id

    def test_as_write(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvas)
        write_instances = instance.as_write()

        assert isinstance(write_instances, IndustrialCanvasApply)

        assert write_instances.canvas.external_id == instance.canvas.external_id
        assert len(write_instances.container_references) == len(instance.container_references) > 0
        assert (
            len(write_instances.fdm_instance_container_references)
            == len(instance.fdm_instance_container_references)
            > 0
        )
        assert len(write_instances.annotations) == len(instance.annotations) > 0
        assert len(write_instances.solution_tags) == len(instance.solution_tags) > 0
