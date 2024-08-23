import pathlib
from collections.abc import Hashable
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import data_modeling as dm

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
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import mock_read_yaml_file


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
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)
        mock_read_yaml_file({"transformation.yaml": yaml.CSafeLoader(self.trafo_yaml).get_data()}, monkeypatch)
        loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
        assert loaded.destination_oidc_credentials is None
        assert loaded.source_oidc_credentials is None

    def test_oidc_auth_load(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config_real: CDFToolConfig,
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

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
        assert loaded.destination_oidc_credentials.dump() == loaded.source_oidc_credentials.dump()
        assert loaded.destination is not None

    def test_oidc_raise_if_invalid(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        resource["authentication"] = {
            "clientId": "{{cicd_clientId}}",
            "clientSecret": "{{cicd_clientSecret}}",
        }

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with pytest.raises(ToolkitYAMLFormatError):
            loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)

    def test_sql_file(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()
        resource.pop("query")
        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with patch.object(TransformationLoader, "_get_query_file", return_value=Path("transformation.sql")):
            with patch.object(pathlib.Path, "read_text", return_value=self.trafo_sql):
                loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
                assert loaded.query == self.trafo_sql

    def test_sql_inline(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        mock_read_yaml_file({"transformation.yaml": resource}, monkeypatch)

        with patch.object(TransformationLoader, "_get_query_file", return_value=None):
            loaded = loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)
            assert loaded.query == resource["query"]

    def test_if_ambiguous(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationLoader(toolkit_client_approval.mock_client, None)

        mock_read_yaml_file({"transformation.yaml": yaml.CSafeLoader(self.trafo_yaml).get_data()}, monkeypatch)

        with pytest.raises(ToolkitYAMLFormatError):
            with patch.object(TransformationLoader, "_get_query_file", return_value=Path("transformation.sql")):
                with patch.object(pathlib.Path, "read_text", return_value=self.trafo_sql):
                    loader.load_resource(Path("transformation.yaml"), cdf_tool_config_real, skip_validation=False)

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
                    (RawDatabaseLoader, RawDatabaseTable("my_db")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table")),
                ],
                id="Transformation to RAW table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = TransformationLoader.get_dependent_items(item)

        assert list(actual) == expected
