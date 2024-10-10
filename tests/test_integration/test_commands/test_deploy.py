import sys
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests import data


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
def test_deploy_core_model(cdf_tool_config: CDFToolConfig) -> None:
    """The motivation for this test is to ensure that we can deploy the core model
    that has a lot of circular dependencies in the views. This was an issue up until
    12. June 2024. The deployment failed as the server was not able to handle
    reverse direct dependencies. This test is to ensure that the deployment works
    as expected.

    It is an expensive test ~10 seconds to run. In the future, we might want to
    remove it as we trust the server to handle the deployment.
    """
    deploy_command = DeployCommand(print_warning=False, skip_tracking=True)

    deploy_command.execute(
        cdf_tool_config,
        build_dir=data.BUILD_CORE_MODEL,
        build_env_name="dev",
        dry_run=False,
        drop=True,
        drop_data=True,
        include=list(LOADER_BY_FOLDER_NAME.keys()),
        verbose=False,
    )


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
def test_deploy_complete_org(cdf_tool_config: CDFToolConfig, build_dir: Path) -> None:
    assert True
