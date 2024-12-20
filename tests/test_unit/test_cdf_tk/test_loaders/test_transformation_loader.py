from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.exceptions import ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import (
    DataModelLoader,
    DataSetsLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
    SpaceLoader,
    TransformationLoader,
    ViewLoader,
)
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalToolkitClient


def _return_none(*args, **kwargs) -> str | None:
    return None


class TestTransformationLoader:
    trafo_yaml = """
externalId: tr_first_transformation
name: 'example:first:transformation'
interval: '{{scheduleHourly}}'
isPaused: true
query: "INLINE"
destination:
  type: 'assets'
ignoreNullFields: true
isPublic: true
conflictMode: upsert
"""

    trafo_sql = "FILE"

    def test_no_auth_load(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)
        filepath = self._create_mock_file(self.trafo_yaml)

        loader._get_query_file = _return_none
        loaded = loader.load_resource_file(filepath, cdf_tool_real)
        assert loaded.destination_oidc_credentials is None
        assert loaded.source_oidc_credentials is None

    def test_oidc_auth_load(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        resource["authentication"] = {
            "clientId": "{{cicd_clientId}}",
            "clientSecret": "{{cicd_clientSecret}}",
            "tokenUri": "{{cicd_tokenUri}}",
            "cdfProjectName": "{{cdfProjectName}}",
            "scopes": "{{cicd_scopes}}",
            "audience": "{{cicd_audience}}",
        }
        filepath = self._create_mock_file(yaml.dump(resource))

        loader._get_query_file = _return_none
        loaded = loader.load_resource_file(filepath, cdf_tool_real)
        assert loaded.destination_oidc_credentials.dump() == loaded.source_oidc_credentials.dump()
        assert loaded.destination is not None

    @staticmethod
    def _create_mock_file(content: str) -> MagicMock:
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = content
        filepath.name = "transformation.yaml"
        filepath.stem = "transformation"
        filepath.parent = Path("path")
        return filepath

    def test_oidc_raise_if_invalid(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        resource["authentication"] = {
            "clientId": "{{cicd_clientId}}",
            "clientSecret": "{{cicd_clientSecret}}",
        }
        filepath = self._create_mock_file(yaml.dump(resource))
        loader._get_query_file = _return_none

        with pytest.raises(ToolkitYAMLFormatError):
            loader.load_resource_file(filepath, cdf_tool_real)

    def test_sql_inline(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        filepath = self._create_mock_file(self.trafo_yaml)
        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        loaded = loader.load_resource_file(filepath, cdf_tool_real)
        assert loaded.query == resource["query"]

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "dataSetExternalId": "ds_my_dataset",
                    "destination": {
                        "type": "instances",
                        "dataModel": {
                            "space": "sp_model_space",
                            "externalId": "my_model",
                            "version": "v1",
                            "destinationType": "assets",
                        },
                        "instanceSpace": "sp_data_space",
                    },
                },
                [
                    (DataSetsLoader, "ds_my_dataset"),
                    (SpaceLoader, "sp_data_space"),
                    (DataModelLoader, dm.DataModelId(space="sp_model_space", external_id="my_model", version="v1")),
                ],
                id="Transformation to data model",
            ),
            pytest.param(
                {
                    "destination": {
                        "type": "nodes",
                        "view": {"space": "sp_space", "externalId": "my_view", "version": "v1"},
                        "instanceSpace": "sp_data_space",
                    }
                },
                [
                    (SpaceLoader, "sp_data_space"),
                    (ViewLoader, dm.ViewId(space="sp_space", external_id="my_view", version="v1")),
                ],
                id="Transformation to nodes ",
            ),
            pytest.param(
                {"destination": {"type": "raw", "database": "my_db", "table": "my_table"}},
                [
                    (RawDatabaseLoader, RawDatabase("my_db")),
                    (RawTableLoader, RawTable("my_db", "my_table")),
                ],
                id="Transformation to RAW table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = TransformationLoader.get_dependent_items(item)

        assert list(actual) == expected
