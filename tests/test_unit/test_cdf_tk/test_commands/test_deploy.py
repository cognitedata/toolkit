from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import ViewLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class TestDeployCommand:
    def test_load_files(self, cdf_tool_mock: CDFToolConfig) -> None:
        path = MagicMock(spec=Path)
        path.name = "my.View.yaml"
        path.read_text.return_value = VIEW_SOURCE_NONE
        cmd = DeployCommand(print_warning=False, skip_tracking=True)

        with pytest.raises(TypeError) as e:
            cmd._load_files(ViewLoader.create_loader(cdf_tool_mock, None), [path], cdf_tool_mock, skip_validation=True)

        assert e.value


VIEW_SOURCE_NONE = """- space: dm_domain_generic
  externalId: Equipment
  name: Equipment
  version: v1
  properties:
    id:
      container:
        space: dm_domain_generic
        externalId: Tag
      containerPropertyIdentifier: id
      type:
        list: false
        collation: ucs_basic
        type: text
      source:
"""
