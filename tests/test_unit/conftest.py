from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import responses
import yaml
from cognite.client import global_config
from cognite.client.credentials import Token
from cognite.client.data_classes import CreatedSession
from cognite.client.data_classes.data_modeling import ContainerList, DataModel, NodeList, View, ViewId
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.canvas import IndustrialCanvas
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import BuildCommand, ModulesCommand, RepoCommand
from cognite_toolkit._cdf_tk.constants import BUILD_ENVIRONMENT_FILE, MODULES
from cognite_toolkit._cdf_tk.data_classes._config_yaml import BuildEnvironment
from cognite_toolkit._cdf_tk.utils import read_yaml_file
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.constants import REPO_ROOT
from tests.data import (
    BUILDABLE_PACKAGE,
    COMPLETE_ORG,
    CORE_CONTAINERS_NO_3D_YAML,
    CORE_NO_3D_YAML,
    EXTRACTOR_VIEWS_YAML,
)
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import PrintCapture

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"
TMP_FOLDER.mkdir(exist_ok=True)
BASE_URL = "http://blabla.cognitedata.com"


@pytest.fixture
def toolkit_client_approval() -> Iterator[ApprovalToolkitClient]:
    with monkeypatch_toolkit_client() as toolkit_client:

        def create_session(*args: Any, **kwargs: Any) -> CreatedSession:
            return CreatedSession(
                id=42,
                status="READY",
                nonce="dummy-nonce",
                type="CLIENT_CREDENTIALS",
            )

        toolkit_client.iam.sessions.create = create_session
        approval_client = ApprovalToolkitClient(toolkit_client)
        yield approval_client


@pytest.fixture(scope="function")
def env_vars_with_client(toolkit_client_approval: ApprovalToolkitClient) -> EnvironmentVariables:
    env_vars = EnvironmentVariables(
        CDF_CLUSTER="bluefield",
        CDF_PROJECT="pytest-project",
        LOGIN_FLOW="client_credentials",
        PROVIDER="entra_id",
        IDP_CLIENT_ID="dummy-123",
        IDP_CLIENT_SECRET="dummy-secret",
        IDP_TENANT_ID="dummy-domain",
    )
    env_vars._client = toolkit_client_approval.mock_client
    return env_vars


@pytest.fixture(scope="session")
def build_tmp_path() -> Iterator[Path]:
    pidid = os.getpid()
    build_folder = TMP_FOLDER / f"build-{pidid}"
    if build_folder.exists():
        shutil.rmtree(build_folder, ignore_errors=True)
        build_folder.mkdir(exist_ok=True)
    yield build_folder
    shutil.rmtree(build_folder, ignore_errors=True)


@pytest.fixture(scope="session")
def local_tmp_repo_path() -> Iterator[Path]:
    pidid = os.getpid()
    repo_path = TMP_FOLDER / f"pytest-repo-{pidid}"
    repo_path.mkdir(exist_ok=True)
    RepoCommand(silent=True, skip_git_verify=True).init(repo_path, host="GitHub")
    yield repo_path
    shutil.rmtree(repo_path, ignore_errors=True)


def init_organization_dir(organization_dir: Path, module_source_dir: Path) -> None:
    ModulesCommand(silent=True, module_source_dir=module_source_dir).init(
        organization_dir,
        select_all=True,
        clean=True,
    )


@pytest.fixture(scope="session")
def complete_org_dir(
    local_tmp_repo_path: Path,
) -> None:
    organization_dir = local_tmp_repo_path / "pytest-org"
    init_organization_dir(organization_dir, COMPLETE_ORG / MODULES)
    return organization_dir


@pytest.fixture(scope="session")
def complete_org_dir_mutable(
    local_tmp_repo_path: Path,
) -> Path:
    """This is used in tests were the source module files are modified. For example, cdf pull commands."""
    organization_dir = local_tmp_repo_path / "pytest-org-mutable"
    init_organization_dir(organization_dir, COMPLETE_ORG / MODULES)
    return organization_dir


@pytest.fixture(scope="session")
def buildable_modules(local_tmp_repo_path: Path) -> Path:
    organization_dir = local_tmp_repo_path / "legacy-pytest-org"
    init_organization_dir(organization_dir, BUILDABLE_PACKAGE / MODULES)
    return organization_dir


@pytest.fixture
def buildable_modules_mutable(
    local_tmp_repo_path: Path,
) -> Path:
    """This is used in tests were the source module files are modified. For example, cdf pull commands."""
    organization_dir = local_tmp_repo_path / "legacy-pytest-org-mutable"
    init_organization_dir(organization_dir, BUILDABLE_PACKAGE / MODULES)
    return organization_dir


@pytest.fixture
def build_environment(
    build_tmp_path: Path,
    complete_org_dir: Path,
    env_vars_with_client: EnvironmentVariables,
) -> BuildEnvironment:
    """Fixture that builds modules and returns the BuildEnvironment."""
    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=complete_org_dir,
        build_dir=build_tmp_path,
        selected=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        build_env_name="dev",
        on_error="raise",
    )
    return BuildEnvironment.load(read_yaml_file(build_tmp_path / BUILD_ENVIRONMENT_FILE), "dev", "build")


@pytest.fixture
def capture_print(monkeypatch: MonkeyPatch) -> PrintCapture:
    capture = PrintCapture()
    toolkit_path = REPO_ROOT / "cognite_toolkit"
    monkeypatch.setattr("cognite_toolkit._cdf.print", capture)

    # Monkeypatch all print functions in the toolkit automatically
    for folder in ["_cdf_tk", "_api"]:
        for py_file in (toolkit_path / folder).rglob("*.py"):
            file_path = py_file.relative_to(toolkit_path)
            module_path = f"{'.'.join(['cognite_toolkit', *file_path.parts[:-1]])}.{file_path.stem}"
            try:
                monkeypatch.setattr(f"{module_path}.print", capture)
            except AttributeError:
                # The module does not have a print function
                continue
            except ImportError:
                raise

    return capture


@pytest.fixture
def disable_gzip():
    old = global_config.disable_gzip
    global_config.disable_gzip = True
    yield
    global_config.disable_gzip = old


@pytest.fixture
def disable_pypi_check():
    old = global_config.disable_pypi_version_check
    global_config.disable_pypi_version_check = True
    yield
    global_config.disable_pypi_version_check = old


@pytest.fixture
def toolkit_config():
    return ToolkitClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        max_workers=1,
        timeout=10,
        credentials=Token("abc"),
    )


@pytest.fixture
def rsps() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture(scope="session")
def asset_centric_canvas() -> tuple[IndustrialCanvas, NodeList[InstanceSource]]:
    canvas = IndustrialCanvas.load(
        {
            "annotations": [],
            "canvas": [
                {
                    "createdTime": 1751540227230,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540558717,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "Canvas/v7": {
                                "createdBy": "aGQ-cBXUBY6bmmxqIdkFoA",
                                "isArchived": None,
                                "isLocked": None,
                                "name": "Asset-centric1",
                                "solutionTags": None,
                                "sourceCanvasId": None,
                                "updatedAt": "2025-07-03T11:02:37.733+00:00",
                                "updatedBy": "aGQ-cBXUBY6bmmxqIdkFoA",
                                "visibility": "public",
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 14,
                }
            ],
            "containerReferences": [
                {
                    "createdTime": 1751540264906,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_cf372b29-3012-49ff-8daf-5043404c23d7",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540264906,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "asset",
                                "height": 357,
                                "id": "cf372b29-3012-49ff-8daf-5043404c23d7",
                                "label": "Kelmarsh 6",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 3840956528416998,
                                "resourceSubId": None,
                                "width": 600,
                                "x": 0,
                                "y": 0,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 1,
                },
                {
                    "createdTime": 1751540275336,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_09d58ddf-bebb-4e4d-96db-1702da76a016",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540275336,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "timeseries",
                                "height": 400,
                                "id": "09d58ddf-bebb-4e4d-96db-1702da76a016",
                                "label": "Hub temperature, standard deviation (Â°C)",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 11978459264156,
                                "resourceSubId": None,
                                "width": 700,
                                "x": 700,
                                "y": 0,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 1,
                },
                {
                    "createdTime": 1751540544349,
                    "externalId": "495af88f-fe1d-403d-91b1-76ef9f80f265_5e2bf845-103c-4c17-8549-9f20329b7f98",
                    "instanceType": "node",
                    "lastUpdatedTime": 1751540558717,
                    "properties": {
                        "cdf_industrial_canvas": {
                            "ContainerReference/v2": {
                                "chartsId": None,
                                "containerReferenceType": "event",
                                "height": 500,
                                "id": "5e2bf845-103c-4c17-8549-9f20329b7f98",
                                "label": "b18cdf8e-6568-4e2a-a267-535eb52f41bf",
                                "maxHeight": None,
                                "maxWidth": None,
                                "resourceId": 9004025980300864,
                                "resourceSubId": None,
                                "width": 600,
                                "x": -10,
                                "y": 418,
                            }
                        }
                    },
                    "space": "IndustrialCanvasInstanceSpace",
                    "version": 4,
                },
            ],
        }
    )
    mapping = NodeList[InstanceSource](
        [
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_asset",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="asset",
                id_=3840956528416998,
                preferred_consumer_view_id=ViewId("my_space", "DoctrinoAsset", "v1"),
            ),
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_timeseries",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="timeseries",
                id_=11978459264156,
                preferred_consumer_view_id=ViewId("my_space", "DoctrinoTimeSeries", "v1"),
            ),
            InstanceSource(
                space="MyNewInstanceSpace",
                external_id="my_event",
                version=1,
                last_updated_time=1,
                created_time=1,
                resource_type="event",
                id_=9004025980300864,
            ),
        ]
    )
    return canvas, mapping


@pytest.fixture(scope="session")
def cognite_core_no_3D() -> DataModel[View]:
    """This is a simplified CogniteCore data model without the 3D views.
    In addition, CogniteAsset does not implement CogniteVisualizable (which is also removed).

    Note if you use this fixture in a test, ensure that you do not modify the returned
    data model, as it is shared between tests.
    """
    return DataModel.load(CORE_NO_3D_YAML.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def cognite_core_containers_no_3D() -> ContainerList:
    """This is a simplified list of containers from the cdf_cdm space without CogniteVisualizable.
    In addition, the CogniteAsset container does not require CogniteVisualizable.

    Note if you use this fixture in a test, ensure that you do not modify the returned
    containers, as it is shared between tests.
    """
    return ContainerList.load(CORE_CONTAINERS_NO_3D_YAML.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def cognite_extractor_views() -> list[View]:
    """This is a simplified data model containing only the views used by the extractor.

    Note if you use this fixture in a test, ensure that you do not modify the returned
    data model, as it is shared between tests.
    """
    return [View._load(view) for view in yaml.safe_load(EXTRACTOR_VIEWS_YAML.read_text(encoding="utf-8"))]
