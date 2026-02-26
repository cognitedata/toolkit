from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId, RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DataModelReference,
    SpaceReference,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    NonceCredentials,
    TransformationRequest,
    TransformationResponse,
)
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


class TestTransformationCRUD:
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

        assert loaded.destination_nonce is None
        assert loaded.source_nonce is None

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
destination:
  type: assets
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
        cdf_transformation = TransformationResponse(
            id=1,
            name="my-transformation",
            external_id="my_transformation",
            query=f"{TransformationCRUD._hash_key}: {auth_hash}\nSELECT * FROM my_table",
            ignore_null_fields=True,
            created_time=1,
            last_updated_time=1,
            is_public=True,
            conflict_mode="upsert",
            destination={"type": "assets"},
            owner="test",
            owner_is_current_user=True,
            has_source_oidc_credentials=False,
            has_destination_oidc_credentials=False,
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
                    (DataSetsCRUD, ExternalId(external_id="ds_my_dataset")),
                    (SpaceCRUD, SpaceReference(space="sp_data_space")),
                    (DataModelCRUD, DataModelReference(space="sp_model_space", external_id="my_model", version="v1")),
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
                    (SpaceCRUD, SpaceReference(space="sp_data_space")),
                    (ViewCRUD, ViewReference(space="sp_space", external_id="my_view", version="v1")),
                ],
                id="Transformation to nodes ",
            ),
            pytest.param(
                {"destination": {"type": "raw", "database": "my_db", "table": "my_table"}},
                [
                    (RawDatabaseCRUD, RawDatabaseId(name="my_db")),
                    (RawTableCRUD, RawTableId(db_name="my_db", name="my_table")),
                ],
                id="Transformation to RAW table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = TransformationCRUD.get_dependent_items(item)

        assert list(actual) == expected

    def test_create_session_nonce_error(self) -> None:
        transformations = [
            TransformationRequest(
                external_id=f"transformation_{i}",
                name=f"Transformation {i}",
                ignore_null_fields=True,
                query="SELECT 1",
                destination_nonce=NonceCredentials(session_id=123, nonce="nonce_value", cdf_project_name="project"),
                source_nonce=NonceCredentials(session_id=123, nonce="nonce_value", cdf_project_name="project"),
            )
            for i in range(3)
        ]

        def create_transformations(transformation: list[TransformationRequest]) -> list[TransformationResponse]:
            if len(transformation) > 1:
                raise ToolkitAPIError(
                    message="Failed to bind session using nonce for: 123",
                    code=400,
                )
            return [
                TransformationResponse(
                    id=i,
                    external_id=t.external_id,
                    name=t.name,
                    ignore_null_fields=t.ignore_null_fields,
                    query=t.query or "",
                    created_time=1,
                    last_updated_time=1,
                    is_public=True,
                    conflict_mode="upsert",
                    destination={"type": "assets"},
                    owner="test",
                    owner_is_current_user=True,
                    has_source_oidc_credentials=False,
                    has_destination_oidc_credentials=False,
                )
                for i, t in enumerate(transformation)
            ]

        with monkeypatch_toolkit_client() as client:
            client.tool.transformations.create.side_effect = create_transformations
            crud = TransformationCRUD(client, None, None)
            created = crud.create(transformations)

            assert [t.external_id for t in created] == [t.external_id for t in transformations]
