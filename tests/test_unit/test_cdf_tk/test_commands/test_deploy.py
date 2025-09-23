from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.cruds import ResourceWorker, ViewCRUD
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


class TestDeployCommand:
    def test_load_files(self, env_vars_with_client: EnvironmentVariables) -> None:
        path = MagicMock(spec=Path)
        path.name = "my.View.yaml"
        path.read_text.return_value = VIEW_SOURCE_NONE
        worker = ResourceWorker(ViewCRUD.create_loader(env_vars_with_client.get_client()), "deploy")

        with pytest.raises(TypeError) as e:
            worker.prepare_resources([path], environment_variables={}, is_dry_run=True, verbose=False)

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
