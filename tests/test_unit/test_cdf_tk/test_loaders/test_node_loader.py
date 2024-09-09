from pathlib import Path

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes.data_modeling import NodeApply

from cognite_toolkit._cdf_tk.loaders import NodeLoader
from cognite_toolkit._cdf_tk.loaders.data_classes import NodeAPICall, NodeApplyListWithCall
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.utils import mock_read_yaml_file


class TestNodeLoader:
    @pytest.mark.parametrize(
        "yamL_raw, expected",
        [
            pytest.param(
                """space: my_space
externalId: my_external_id""",
                NodeApplyListWithCall([NodeApply("my_space", "my_external_id")]),
                id="Single node no API call",
            ),
            pytest.param(
                """- space: my_space
  externalId: my_first_node
- space: my_space
  externalId: my_second_node
""",
                NodeApplyListWithCall(
                    [
                        NodeApply("my_space", "my_first_node"),
                        NodeApply("my_space", "my_second_node"),
                    ]
                ),
                id="Multiple nodes no API call",
            ),
            pytest.param(
                """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
node:
  space: my_space
  externalId: my_external_id""",
                NodeApplyListWithCall([NodeApply("my_space", "my_external_id")], NodeAPICall(True, False, True)),
                id="Single node with API call",
            ),
            pytest.param(
                """autoCreateDirectRelations: true
skipOnVersionConflict: false
replace: true
nodes:
- space: my_space
  externalId: my_first_node
- space: my_space
  externalId: my_second_node
    """,
                NodeApplyListWithCall(
                    [
                        NodeApply("my_space", "my_first_node"),
                        NodeApply("my_space", "my_second_node"),
                    ],
                    NodeAPICall(True, False, True),
                ),
                id="Multiple nodes with API call",
            ),
        ],
    )
    def test_load_nodes(
        self,
        yamL_raw: str,
        expected: NodeApplyListWithCall,
        cdf_tool_mock: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = NodeLoader.create_loader(cdf_tool_mock, None)
        mock_read_yaml_file({"my_node.yaml": yaml.safe_load(yamL_raw)}, monkeypatch)
        loaded = loader.load_resource(Path("my_node.yaml"), cdf_tool_mock, skip_validation=True)

        assert loaded.dump() == expected.dump()
