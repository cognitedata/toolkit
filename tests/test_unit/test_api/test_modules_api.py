from __future__ import annotations

import pytest

from cognite_toolkit._api import CogniteToolkit
from cognite_toolkit._api import data_classes as dc
from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, iterate_modules
from tests.constants import REPO_ROOT
from tests.test_unit.approval_client import ApprovalToolkitClient

_ALL_MODULE_NAMES = [
    module_path.name
    for module_path, _ in iterate_modules(REPO_ROOT / "cognite_toolkit")
    if COGNITE_MODULES in module_path.parts
]


@pytest.fixture
def all_modules(cognite_toolkit: CogniteToolkit) -> dc.ModuleMetaList:
    return cognite_toolkit.modules.list()


class TestModulesAPI:
    def test_list_modules(self, cognite_toolkit: CogniteToolkit) -> None:
        expected_modules = {
            modul_path.name for modul_path, _ in iterate_modules(REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES)
        }

        modules = cognite_toolkit.modules.list()

        assert isinstance(modules, dc.ModuleMetaList)
        assert set(modules.names) == expected_modules

        cdf_auth_readwrite_all = modules["cdf_auth_readwrite_all"]
        assert cdf_auth_readwrite_all.name == "cdf_auth_readwrite_all"
        assert len(cdf_auth_readwrite_all.variables) == 2

    def test_deploy_module_with_modified_variables(
        # THE CDFToolConfig fixture is used as it sets the necessary environment variables
        self,
        cognite_toolkit: CogniteToolkit,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config: CDFToolConfig,
    ) -> None:
        module = cognite_toolkit.modules.retrieve("cdf_auth_readwrite_all")
        assert isinstance(module, dc.ModuleMeta)
        module.variables["readwrite_source_id"].value = "123"
        module.variables["readonly_source_id"].value = "456"

        cognite_toolkit.modules.deploy(module)

        dumped = toolkit_client_approval.dump(sort=True)
        groups = dumped["Group"]

        assert len(groups) == 2
        assert groups[0]["name"] == "gp_admin_read_write"
        assert groups[0]["sourceId"] == "123"
        assert groups[1]["name"] == "gp_admin_readonly"
        assert groups[1]["sourceId"] == "456"

    def test_deploy_module_only_auth(
        # THE CDFToolConfig fixture is used as it sets the necessary environment variables
        self,
        cognite_toolkit: CogniteToolkit,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config: CDFToolConfig,
    ):
        module = cognite_toolkit.modules.retrieve("cdf_infield_common")

        cognite_toolkit.modules.deploy(module, include={"auth"})

        dumped = toolkit_client_approval.dump(sort=False)

        assert len(dumped) == 1
        assert "Group" in dumped

    @pytest.mark.parametrize("module_name", _ALL_MODULE_NAMES)
    def test_deploy_module(
        self,
        module_name: str,
        cognite_toolkit: CogniteToolkit,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config: CDFToolConfig,
    ) -> None:
        toolkit_client_approval.return_verify_resources = True
        module = cognite_toolkit.modules.retrieve(module_name)

        cognite_toolkit.modules.deploy(module, verbose=True)

        assert True

    @pytest.mark.parametrize("module_name", _ALL_MODULE_NAMES)
    def test_clean_module(
        self,
        module_name: str,
        cognite_toolkit: CogniteToolkit,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_config: CDFToolConfig,
    ):
        toolkit_client_approval.return_verify_resources = True
        module = cognite_toolkit.modules.retrieve(module_name)

        cognite_toolkit.modules.clean(module, verbose=True)

        assert True
