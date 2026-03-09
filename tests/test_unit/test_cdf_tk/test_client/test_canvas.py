from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    SOLUTION_TAG_SPACE,
    IndustrialCanvasRequest,
    IndustrialCanvasResponse,
)
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestIndustrialCanvasDataClass:
    def test_create_backup(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvasRequest)

        backup = instance.create_backup()

        original_ids = set(instance.as_ids())
        backup_ids = set(backup.as_ids())
        assert len(original_ids) == len(backup_ids)
        overlapping_ids = original_ids.intersection(backup_ids)
        assert not overlapping_ids

        original_ids = set(instance.as_ids(include_solution_tags=True))
        backup_ids = set(backup.as_ids(include_solution_tags=True))
        overlapping_ids = original_ids.intersection(backup_ids)
        solution_tags_ids = {
            NodeId(space=SOLUTION_TAG_SPACE, external_id=tag.external_id) for tag in instance.solution_tags
        }
        assert solution_tags_ids == overlapping_ids, "Expected overlapping IDs to be solution tags IDs only"

    def test_as_instances(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvasRequest)

        node_or_edges = instance.dump_instances(include_solution_tags=True)

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
        assert len(instance.as_ids(include_solution_tags=False)) == expected_instance_count - len(
            instance.solution_tags
        )
        assert len(instance.as_ids(include_solution_tags=True)) == expected_instance_count
        assert instance.as_id() == NodeId(space=CANVAS_INSTANCE_SPACE, external_id=instance.external_id)

    def test_as_write(self) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(IndustrialCanvasResponse)
        write_instances = instance.as_request_resource()

        assert isinstance(write_instances, IndustrialCanvasRequest)

        assert write_instances.external_id == instance.external_id
        assert len(write_instances.container_references) == len(instance.container_references) > 0
        assert (
            len(write_instances.fdm_instance_container_references)
            == len(instance.fdm_instance_container_references)
            > 0
        )
        assert len(write_instances.annotations) == len(instance.annotations) > 0
        assert len(write_instances.solution_tags) == len(instance.solution_tags) > 0
