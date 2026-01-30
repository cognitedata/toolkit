import os
import zipfile
from pathlib import Path
from typing import cast

import pytest
from cognite.client import global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadata, FileMetadataWrite
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from tests.data import THREE_D_He2_FBX_ZIP
from tests_smoke.constants import SMOKE_SPACE

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def toolkit_client_config() -> ToolkitClientConfig:
    load_dotenv(REPO_ROOT / ".env", override=True)
    cdf_cluster = os.environ["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        audience=f"https://{cdf_cluster}.cognitedata.com",
    )
    global_config.disable_pypi_version_check = True
    return ToolkitClientConfig(
        client_name="cdf-toolkit-integration-tests",
        base_url=f"https://{cdf_cluster}.cognitedata.com",
        project=os.environ["CDF_PROJECT"],
        credentials=credentials,
        is_strict_validation=False,
    )


@pytest.fixture(scope="session")
def toolkit_client(toolkit_client_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(toolkit_client_config, enable_set_pending_ids=True)


@pytest.fixture(scope="session")
def smoke_dataset(toolkit_client: ToolkitClient) -> DataSetResponse:
    client = toolkit_client
    dataset_external_id = ExternalId(external_id="toolkit_smoke_test_dataset")
    if dataset := client.tool.datasets.retrieve([dataset_external_id]):
        return dataset[0]
    return client.tool.datasets.create(
        [
            DataSetRequest(
                name="Toolkit Smoke Test Dataset",
                external_id=dataset_external_id.external_id,
                description="Dataset for Cognite Toolkit migration integration tests",
                metadata={
                    "source": "ToolkitSmokeTests",
                },
            )
        ]
    )[0]


@pytest.fixture(scope="session")
def smoke_space(toolkit_client: ToolkitClient) -> "Space":
    client = toolkit_client

    space_external_id = SMOKE_SPACE
    if space := client.data_modeling.spaces.retrieve(space_external_id):
        return space
    return client.data_modeling.spaces.apply(
        SpaceApply(
            name="Toolkit Smoke Test Space",
            space=space_external_id,
            description="Space for Cognite Toolkit migration integration tests",
        )
    )


@pytest.fixture(scope="session")
def three_d_file(toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse) -> FileMetadata:
    client = toolkit_client
    meta = FileMetadataWrite(
        name="he2.fbx",
        data_set_id=smoke_dataset.id,
        external_id="toolkit_3d_model_test_file_external_id",
        metadata={"source": "integration_test"},
        mime_type="application/octet-stream",
        source="3d-models",
    )
    read = cast(FileMetadata | None, client.files.retrieve(external_id=meta.external_id))
    if read and read.uploaded is True:
        return read
    if read is None:
        read, _ = client.files.create(meta)
    with zipfile.ZipFile(THREE_D_He2_FBX_ZIP, mode="r") as zip_ref:
        file_data = zip_ref.read("he2.fbx")
        read = client.files.upload_content_bytes(file_data, external_id=meta.external_id)
    assert read.uploaded is True
    return read
