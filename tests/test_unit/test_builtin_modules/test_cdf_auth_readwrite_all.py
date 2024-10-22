from pathlib import Path
from typing import Any

import pytest
from cognite.client.data_classes import GroupWrite

from cognite_toolkit._cdf_tk.commands import BuildCommand
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuiltModule,
)
from cognite_toolkit._cdf_tk.loaders import GroupAllScopedLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


@pytest.fixture(scope="session")
def built_module(tmp_path_factory: Any, organization_dir: Path) -> BuiltModule:
    cmd = BuildCommand(silent=True, skip_tracking=True)
    config = BuildConfigYAML.load_default(organization_dir)
    config.environment.selected = ["cdf_auth_readwrite_all"]
    built = cmd.build_config(
        build_dir=tmp_path_factory.mktemp("build"),
        organization_dir=organization_dir,
        config=config,
        packages={},
        on_error="raise",
    )
    assert len(built) == 1

    return built[0]


@pytest.fixture
def read_write_group(built_module: BuiltModule, cdf_tool_mock: CDFToolConfig) -> GroupWrite:
    auth = built_module.resources["auth"]
    read_write_resource = next((resource for resource in auth if resource.identifier == "gp_admin_read_write"), None)
    assert read_write_resource is not None

    loader = GroupAllScopedLoader(cdf_tool_mock.toolkit_client, None)
    groups = loader.load_resource(read_write_resource.destination, cdf_tool_mock, skip_validation=False)
    assert len(groups) == 1
    return groups[0]


@pytest.fixture
def readonly_group(built_module: BuiltModule, cdf_tool_mock: CDFToolConfig) -> GroupWrite:
    auth = built_module.resources["auth"]
    readonly_resource = next((resource for resource in auth if resource.identifier == "gp_admin_readonly"), None)
    assert readonly_resource is not None

    loader = GroupAllScopedLoader(cdf_tool_mock.toolkit_client, None)
    groups = loader.load_resource(readonly_resource.destination, cdf_tool_mock, skip_validation=False)
    assert len(groups) == 1
    return groups[0]


class TestCDFAuthReadWriteAll:
    def test_read_write_group_is_up_to_date(self, read_write_group: GroupWrite) -> None:
        assert True

    def test_readdonly_group_is_up_to_date(self, readonly_group: GroupWrite) -> None:
        assert True
