import os
import shutil
from pathlib import Path

import pytest
from cognite.client import CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands import CollectCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.constants import REPO_ROOT

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"


@pytest.fixture(scope="session")
def toolkit_client_config() -> ToolkitClientConfig:
    load_dotenv(REPO_ROOT / ".env", override=True)
    # Ensure that we do not collect data during tests
    cmd = CollectCommand()
    cmd.execute(action="opt-out")

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
        # We cannot commit auth to WorkflowTrigger and FunctionSchedules.
        is_strict_validation=False,
    )


@pytest.fixture(scope="session")
def cognite_client(toolkit_client_config: ToolkitClientConfig) -> CogniteClient:
    return CogniteClient(toolkit_client_config)


@pytest.fixture(scope="session")
def toolkit_client(toolkit_client_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(toolkit_client_config)


@pytest.fixture(scope="session")
def env_vars(toolkit_client: ToolkitClient) -> EnvironmentVariables:
    env_vars = EnvironmentVariables.create_from_environment()
    # Ensure we use the client above that has CLIENT NAME set to the test name
    env_vars._client = toolkit_client
    return env_vars


@pytest.fixture(scope="session")
def toolkit_space(cognite_client: CogniteClient) -> Space:
    return cognite_client.data_modeling.spaces.apply(SpaceApply(space="toolkit_test_space"))


@pytest.fixture
def build_dir() -> Path:
    pidid = os.getpid()
    build_path = TMP_FOLDER / f"build-{pidid}"
    build_path.mkdir(exist_ok=True, parents=True)
    yield build_path
    shutil.rmtree(build_path, ignore_errors=True)


@pytest.fixture(scope="session")
def dev_cluster_client() -> ToolkitClient | None:
    """Returns a ToolkitClient configured for the development cluster."""
    dev_cluster_env = REPO_ROOT / "dev-cluster.env"
    if not dev_cluster_env.exists():
        pytest.skip("dev-cluster.env file not found, skipping tests that require dev cluster client.")
        return None
    env_content = dev_cluster_env.read_text(encoding="utf-8")
    env_vars = dict(
        line.strip().split("=")
        for line in env_content.splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    )
    cdf_cluster = env_vars["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=env_vars["IDP_TOKEN_URL"],
        client_id=env_vars["IDP_CLIENT_ID"],
        client_secret=env_vars["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        audience=f"https://{cdf_cluster}.cognitedata.com",
    )
    config = ToolkitClientConfig(
        client_name="cdf-toolkit-integration-tests",
        base_url=f"https://{cdf_cluster}.cognitedata.com",
        project=env_vars["CDF_PROJECT"],
        credentials=credentials,
        is_strict_validation=False,
    )
    return ToolkitClient(config, enable_set_pending_ids=True)
