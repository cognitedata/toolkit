import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from cognite.client import data_modeling as dm
from rich import print

from cognite_toolkit._cdf_tk.commands import (
    BuildV2Command,
    DeployOptions,
    DeployV2Command,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_CRUD_BY_FOLDER_NAME,
    RESOURCE_CRUD_LIST,
    ExternalDataSourceIO,
    HostedExtractorDestinationIO,
    HostedExtractorSourceIO,
    ResourceWorker,
)
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_product import DataProductIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_product_version import DataProductVersionIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.rulesets import RuleSetIO, RuleSetVersionIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests import data


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
@pytest.mark.usefixtures("simulator", "simulator_integration", "three_d_file")
def test_deploy_complete_org(env_vars: EnvironmentVariables, build_dir: Path) -> None:
    build = BuildV2Command(silent=True, skip_tracking=True)

    build.build(
        client=env_vars.get_client(),
        parameters=BuildParameters(
            organization_dir=data.COMPLETE_ORG,
            build_dir=build_dir,
            config_yaml=data.COMPLETE_ORG / "config.dev.yaml",
        ),
    )

    deploy_command = DeployV2Command(silent=False, skip_tracking=True)
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    with patch.dict(
        os.environ,
        {"EVENTHUB_CLIENT_ID": client_id, "EVENTHUB_CLIENT_SECRET": client_secret},
    ):
        deploy_command.deploy(
            user_build_dir=build_dir,
            env_vars=env_vars,
            options=DeployOptions(
                cdf_project=env_vars.CDF_PROJECT,
                dry_run=False,
                drop=False,
                drop_data=False,
                force_update=False,
                include=None,
                verbose=True,
                environment_variables=env_vars.dump(),
            ),
        )

    changed_resources = get_changed_resources(env_vars, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="We only run this test on Python 3.11+ to avoid parallelism issues"
)
@pytest.mark.usefixtures("simulator", "simulator_integration", "three_d_file")
def test_deploy_complete_org_alpha(env_vars: EnvironmentVariables, build_dir: Path) -> None:
    build = BuildV2Command(silent=True, skip_tracking=True)

    build.build(
        client=env_vars.get_client(),
        parameters=BuildParameters(
            organization_dir=data.COMPLETE_ORG_ALPHA_FLAGS,
            build_dir=build_dir,
            config_yaml=data.COMPLETE_ORG_ALPHA_FLAGS / "config.dev.yaml",
        ),
    )

    deploy_command = DeployV2Command(silent=False, skip_tracking=True)
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    # Data Products and Rule Sets APIs are not yet available on the test server.
    # External data sources reject dummy OneLake credentials (400 Invalid body).
    _skip_cruds = {DataProductIO, DataProductVersionIO, RuleSetIO, RuleSetVersionIO, ExternalDataSourceIO}
    with (
        patch.dict(
            os.environ,
            {"EVENTHUB_CLIENT_ID": client_id, "EVENTHUB_CLIENT_SECRET": client_secret},
        ),
        patch.dict(
            "cognite_toolkit._cdf_tk.commands.deploy_v2.command.RESOURCE_CRUD_BY_FOLDER_NAME",
            {f: [c for c in cs if c not in _skip_cruds] for f, cs in RESOURCE_CRUD_BY_FOLDER_NAME.items()},
            clear=True,
        ),
    ):
        deploy_command.deploy(
            env_vars=env_vars,
            user_build_dir=build_dir,
            options=DeployOptions(
                cdf_project=env_vars.CDF_PROJECT,
                dry_run=False,
                drop=False,
                drop_data=False,
                force_update=False,
                include=None,
                verbose=True,
                environment_variables=env_vars.dump(),
            ),
        )

    changed_resources = get_changed_resources(env_vars, build_dir)
    assert not changed_resources, "Redeploying the same resources should not change anything"


def get_changed_resources(env_vars: EnvironmentVariables, build_dir: Path) -> dict[str, set[Any]]:
    changed_resources: dict[str, set[Any]] = {}
    client = env_vars.get_client()
    print("Looking for changed resources ...")
    for loader_cls in RESOURCE_CRUD_LIST:
        if loader_cls in {HostedExtractorSourceIO, HostedExtractorDestinationIO}:
            # These resources we have no way of knowing if they have changed. So they are always redeployed.
            continue
        if loader_cls in {DataProductIO, DataProductVersionIO, RuleSetIO, RuleSetVersionIO, ExternalDataSourceIO}:
            # Data Products and Rule Sets APIs are not yet available on the test server.
            # External data sources reject dummy OneLake credentials (400 Invalid body).
            continue
        loader = loader_cls.create_loader(client, build_dir)

        worker = ResourceWorker(loader, "deploy")
        files = worker.load_files()
        resources = worker.prepare_resources(files, environment_variables=env_vars.dump(), verbose=True)
        if changed := (set(loader.get_ids(resources.to_update)) - {dm.NodeId("sp_nodes", "MyExtendedFile")}):
            # We do not have a way to get CogniteFile extensions. This is a workaround to avoid the test failing.
            changed_resources[loader.display_name] = changed
            worker.prepare_resources(files, environment_variables=env_vars.dump(), verbose=True)

    return changed_resources
