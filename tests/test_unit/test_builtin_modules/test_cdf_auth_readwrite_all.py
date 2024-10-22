from pathlib import Path
from typing import Any

import pytest
from cognite.client._api.iam import IAMAPI
from cognite.client.data_classes import GroupWrite, capabilities

from cognite_toolkit._cdf_tk.commands import BuildCommand
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuiltModule,
)
from cognite_toolkit._cdf_tk.loaders import GroupAllScopedLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig

SKIP_ACLS = frozenset(
    {
        capabilities.LegacyGenericsAcl,
        capabilities.LegacyModelHostingAcl,
        capabilities.ExperimentsAcl,
    }
)

READ_ACTIONS = frozenset(
    {
        "READ",
        "USE",
        "LIST",
        "MEMBEROF",
    }
)


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


def get_all_capabilities(readonly: bool = False) -> list[capabilities.Capability]:
    all_capabilities: list[capabilities.Capability] = []
    for name, capability_cls in capabilities._CAPABILITY_CLASS_BY_NAME.items():
        if capability_cls in SKIP_ACLS:
            continue
        actions = list(capability_cls.Action)
        if readonly:
            actions = [action for action in actions if action.name in READ_ACTIONS]
        available_scopes = vars(capability_cls.Scope)
        if "All" not in available_scopes:
            raise ValueError(f"Capability {capability_cls} does not have a scope named 'All'")
        scope = available_scopes["All"]

        capability = capability_cls(actions=actions, scope=scope())
        all_capabilities.append(capability)
    return all_capabilities


class TestCDFAuthReadWriteAll:
    def test_read_write_group_is_up_to_date(self, read_write_group: GroupWrite) -> None:
        missing_capabilities = IAMAPI.compare_capabilities(
            read_write_group.capabilities, get_all_capabilities(readonly=False)
        )
        assert not missing_capabilities, f"Missing {len(missing_capabilities)} capabilities: {missing_capabilities}"

    def test_readdonly_group_is_up_to_date(self, readonly_group: GroupWrite) -> None:
        missing_capabilities = IAMAPI.compare_capabilities(
            readonly_group.capabilities, get_all_capabilities(readonly=True)
        )
        assert not missing_capabilities, f"Missing {len(missing_capabilities)} capabilities: {missing_capabilities}"
