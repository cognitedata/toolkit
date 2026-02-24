import os
import zipfile
from pathlib import Path

import pytest
from cognite.client import global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
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
    return ToolkitClient(toolkit_client_config)


@pytest.fixture(scope="session")
def smoke_dataset(toolkit_client: ToolkitClient) -> DataSetResponse:
    client = toolkit_client
    dataset_external_id = ExternalId(external_id="toolkit_smoke_test_dataset")
    if dataset := client.tool.datasets.retrieve([dataset_external_id], ignore_unknown_ids=True):
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
def three_d_file(toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse) -> FileMetadataResponse:
    client = toolkit_client
    mime_type = "application/octet-stream"
    meta = FileMetadataRequest(
        name="he2.fbx",
        data_set_id=smoke_dataset.id,
        external_id="toolkit_3d_model_test_file_external_id",
        metadata={"source": "integration_test"},
        mime_type=mime_type,
        source="3d-models",
    )
    read = client.tool.filemetadata.retrieve([meta.as_id()], ignore_unknown_ids=True)
    if read and read[0].uploaded is True:
        return read[0]

    created_list = client.tool.filemetadata.create([meta], overwrite=True)
    created = created_list[0]
    if not created.upload_url:
        raise AssertionError("Expected upload URL to be present for created file metadata")
    with zipfile.ZipFile(THREE_D_He2_FBX_ZIP, mode="r") as zip_ref:
        file_data = zip_ref.read("he2.fbx")
        result = client.http_client.request_single_retries(
            RequestMessage(
                endpoint_url=created.upload_url,
                method="PUT",
                content_type=mime_type,
                data_content=file_data,
            )
        )
        if not isinstance(result, SuccessResponse):
            raise AssertionError(f"File upload failed. Result: {result!s}")
    return created
