import os
import sys
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME, RESOURCE_LOADER_LIST, ResourceWorker
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests import data


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
def test_deploy_complete_org(cdf_tool_config: CDFToolConfig, build_dir: Path) -> None:
    build = BuildCommand(silent=True, skip_tracking=True)

    build.execute(
        verbose=False,
        organization_dir=data.COMPLETE_ORG,
        build_dir=build_dir,
        build_env_name="dev",
        no_clean=False,
        selected=None,
        ToolGlobals=cdf_tool_config,
    )

    deploy_command = DeployCommand(silent=False, skip_tracking=True)
    cdf_tool_config._environ["EVENTHUB_CLIENT_ID"] = os.environ["IDP_CLIENT_ID"]
    cdf_tool_config._environ["EVENTHUB_CLIENT_SECRET"] = os.environ["IDP_CLIENT_SECRET"]

    deploy_command.execute(
        cdf_tool_config,
        build_dir=build_dir,
        build_env_name="dev",
        dry_run=False,
        drop=False,
        drop_data=False,
        force_update=False,
        include=list(LOADER_BY_FOLDER_NAME.keys()),
        verbose=True,
    )

    changed_resources = get_changed_resources(cdf_tool_config, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
def test_deploy_complete_org_alpha(cdf_tool_config: CDFToolConfig, build_dir: Path) -> None:
    build = BuildCommand(silent=True, skip_tracking=True)

    build.execute(
        verbose=False,
        organization_dir=data.COMPLETE_ORG_ALPHA_FLAGS,
        build_dir=build_dir,
        build_env_name="dev",
        no_clean=False,
        selected=None,
        ToolGlobals=cdf_tool_config,
    )

    deploy_command = DeployCommand(silent=False, skip_tracking=True)
    cdf_tool_config._environ["EVENTHUB_CLIENT_ID"] = os.environ["IDP_CLIENT_ID"]
    cdf_tool_config._environ["EVENTHUB_CLIENT_SECRET"] = os.environ["IDP_CLIENT_SECRET"]

    deploy_command.execute(
        cdf_tool_config,
        build_dir=build_dir,
        build_env_name="dev",
        dry_run=False,
        drop=False,
        drop_data=False,
        force_update=False,
        include=list(LOADER_BY_FOLDER_NAME.keys()),
        verbose=True,
    )

    changed_resources = get_changed_resources(cdf_tool_config, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"


def get_changed_resources(cdf_tool_config: CDFToolConfig, build_dir: Path) -> dict[str, list[str]]:
    changed_resources: dict[str, list[str]] = {}
    for loader_cls in RESOURCE_LOADER_LIST:
        loader = loader_cls.create_loader(cdf_tool_config, build_dir)
        worker = ResourceWorker(loader)
        files = worker.load_files()
        _, to_update, *__ = worker.load_resources(files, environment_variables=cdf_tool_config.environment_variables())
        if to_update:
            changed_resources[loader.display_name] = loader.get_ids(to_update)

    return changed_resources
