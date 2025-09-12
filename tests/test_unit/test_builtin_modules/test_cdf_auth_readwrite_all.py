from pathlib import Path
from typing import Any

import pyperclip
import pytest
from cognite.client._api.iam import IAMAPI
from cognite.client.data_classes import GroupWrite, capabilities

from cognite_toolkit._cdf_tk.commands import AuthCommand, BuildCommand
from cognite_toolkit._cdf_tk.cruds import GroupAllScopedCRUD
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    BuiltModule,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

SKIP_ACLS = frozenset(
    {
        capabilities.LegacyGenericsAcl,
        capabilities.LegacyModelHostingAcl,
        capabilities.ExperimentsAcl,
        capabilities.ProjectsAcl,
        capabilities.UserProfilesAcl,
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
def read_write_group(built_module: BuiltModule, env_vars_with_client: EnvironmentVariables) -> GroupWrite:
    auth = built_module.resources["auth"]
    read_write_resource = next((resource for resource in auth if resource.identifier == "gp_admin_read_write"), None)
    assert read_write_resource is not None

    loader = GroupAllScopedCRUD(env_vars_with_client.get_client(), None, None)
    items = loader.load_resource_file(read_write_resource.destination, env_vars_with_client.dump())
    return loader.load_resource(items[0], is_dry_run=True)


@pytest.fixture
def readonly_group(built_module: BuiltModule, env_vars_with_client: EnvironmentVariables) -> GroupWrite:
    auth = built_module.resources["auth"]
    readonly_resource = next((resource for resource in auth if resource.identifier == "gp_admin_readonly"), None)
    assert readonly_resource is not None

    loader = GroupAllScopedCRUD(env_vars_with_client.get_client(), None, None)
    items = loader.load_resource_file(readonly_resource.destination, env_vars_with_client.dump())
    return loader.load_resource(items[0], is_dry_run=True)


def get_all_capabilities(readonly: bool = False) -> list[capabilities.Capability]:
    all_capabilities: list[capabilities.Capability] = []
    for name, capability_cls in capabilities._CAPABILITY_CLASS_BY_NAME.items():
        if capability_cls in SKIP_ACLS:
            continue
        actions = list(capability_cls.Action)
        if readonly:
            actions = [action for action in actions if action.value in READ_ACTIONS]
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
        if missing_capabilities:
            merged = AuthCommand._merge_capabilities(missing_capabilities)
            missing_yaml = yaml_safe_dump([item.dump() for item in merged], indent=2)
            pyperclip.copy(missing_yaml)

        # This test will fail typically after you have updated the cognite-sdk-python package.
        # It checks that all capabilities that are available in the SDK are also available in the group
        # 'gp_admin_read_write'.
        # For your convenience, the missing capabilities will be copied to your clipboard. You
        # need to go to the 'cognite_toolkit/builtin_modules' and find the 'cdf_auth_readwrite_all' module,
        # and update the capabilities in the 'gp_admin_read_write' group.
        assert not missing_capabilities, (
            f"Missing {len(missing_capabilities)} the missing capabilities have been copied to your clipboard."
        )

    def test_readonly_group_is_up_to_date(self, readonly_group: GroupWrite) -> None:
        missing_capabilities = IAMAPI.compare_capabilities(
            readonly_group.capabilities, get_all_capabilities(readonly=True)
        )
        if missing_capabilities:
            merged = AuthCommand._merge_capabilities(missing_capabilities)
            missing_yaml = yaml_safe_dump([item.dump() for item in merged], indent=2)
            pyperclip.copy(missing_yaml)

        # Similar to the previous test, this test will fail typically after you have updated the
        # cognite-sdk-python package.
        # Running this you need to update the 'gp_admin_readonly' group in the 'cdf_auth_readwrite_all' module.
        assert not missing_capabilities, (
            f"Missing {len(missing_capabilities)} the missing capabilities have been copied to your clipboard"
        )
