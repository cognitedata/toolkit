import pytest
from cognite.client.data_classes._base import CogniteResource

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    Canvas,
    CanvasAnnotation,
    CanvasAnnotationApply,
    CanvasApply,
    CogniteSolutionTag,
    CogniteSolutionTagApply,
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
        ],
    )
    def test_dump_reload(self, node_cls: type[CogniteResource]) -> None:
        instance = FakeCogniteResourceGenerator().create_instance(node_cls)
        dumped = instance.dump()
        reloaded = node_cls.load(dumped)

        assert reloaded == instance, f"Expected: {instance}, but got: {reloaded}"
