from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import Transformation
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import (
    DataModelCRUD,
    DataSetsCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceCRUD,
    SpaceCRUD,
    TransformationCRUD,
    ViewCRUD,
)
from cognite_toolkit._cdf_tk.utils import calculate_secure_hash
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
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
        env_vars_with_client: EnvironmentVariables,
    ) -> None:
        loader = TransformationCRUD(toolkit_client_approval.mock_client, None)
        filepath = self._create_mock_file(self.trafo_yaml)

        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert loaded.destination_oidc_credentials is None
        assert loaded.source_oidc_credentials is None

    def test_oidc_auth_load(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationCRUD(toolkit_client_approval.mock_client, None)
        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()
        resource["authentication"] = {
            "clientId": "my-client-id",
            "clientSecret": "my-client-secret",
            "tokenUri": "https://cognite.com/token",
            "cdfProjectName": "my-project",
            "scopes": "USER_IMPERSONATION",
            "audience": "https://cognite.com",
        }
        filepath = self._create_mock_file(yaml.dump(resource))
        resource_id = resource["externalId"]

        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        _ = loader.load_resource(raw_list[0], is_dry_run=False)

        read_credentials = loader._authentication_by_id_operation[(resource_id, "read")]
        write_credentials = loader._authentication_by_id_operation[(resource_id, "write")]

        assert read_credentials.dump() == write_credentials.dump()

    @staticmethod
    def _create_mock_file(content: str) -> MagicMock:
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = content
        filepath.name = "transformation.yaml"
        filepath.stem = "transformation"
        filepath.parent = Path("path")
        return filepath

    def test_auth_unchanged_changed(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        local_content = """name: my-transformation
externalId: my_transformation
ignoreNullFields: true
query: SELECT * FROM my_table
authentication:
  clientId: my-client-id
  clientSecret: my-client-secret
  scopes: USER_IMPERSONATION
  tokenUri: https://cognite.com/token
  cdfProjectName: my-project
        """
        auth_dict = {"authentication": yaml.CSafeLoader(local_content).get_data()["authentication"]}
        auth_hash = calculate_secure_hash(auth_dict, shorten=True)
        cdf_transformation = Transformation(
            name="my-transformation",
            external_id="my_transformation",
            query=f"{TransformationCRUD._hash_key}: {auth_hash}\nSELECT * FROM my_table",
            ignore_null_fields=True,
        )
        with monkeypatch_toolkit_client() as client:
            loader = TransformationCRUD(client, None, None)

        filepath = self._create_mock_file(local_content)
        local_dumped = loader.load_resource_file(filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_transformation, local_dumped)
        assert cdf_dumped == local_dumped

        new_filepath = self._create_mock_file(local_content.replace("my-client-secret", "my-new-client-secret"))
        new_local_dumped = loader.load_resource_file(new_filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_transformation, new_local_dumped)
        assert cdf_dumped != new_local_dumped

    def test_sql_inline(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TransformationCRUD(toolkit_client_approval.mock_client, None)

        filepath = self._create_mock_file(self.trafo_yaml)
        resource = yaml.CSafeLoader(self.trafo_yaml).get_data()

        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)
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
                    (DataSetsCRUD, "ds_my_dataset"),
                    (SpaceCRUD, "sp_data_space"),
                    (DataModelCRUD, dm.DataModelId(space="sp_model_space", external_id="my_model", version="v1")),
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
                    (SpaceCRUD, "sp_data_space"),
                    (ViewCRUD, dm.ViewId(space="sp_space", external_id="my_view", version="v1")),
                ],
                id="Transformation to nodes ",
            ),
            pytest.param(
                {"destination": {"type": "raw", "database": "my_db", "table": "my_table"}},
                [
                    (RawDatabaseCRUD, RawDatabase("my_db")),
                    (RawTableCRUD, RawTable("my_db", "my_table")),
                ],
                id="Transformation to RAW table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = TransformationCRUD.get_dependent_items(item)

        assert list(actual) == expected
