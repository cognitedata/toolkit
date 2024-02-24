from cognite_toolkit import CogniteToolkit
from cognite_toolkit import data_classes as dc
from cognite_toolkit._cdf_tk.templates import COGNITE_MODULES, iterate_modules
from tests.constants import REPO_ROOT
from tests.tests_unit.approval_client import ApprovalCogniteClient


class TestModulesAPI:
    def test_list_modules(self, cognite_toolkit: CogniteToolkit) -> None:
        expected_modules = {
            modul_path.name for modul_path, _ in iterate_modules(REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES)
        }

        modules = cognite_toolkit.modules.list()

        assert isinstance(modules, dc.ModuleList)
        assert set(modules.names) == expected_modules

        cdf_auth_readwrite_all = modules["cdf_auth_readwrite_all"]
        assert cdf_auth_readwrite_all.name == "cdf_auth_readwrite_all"
        assert len(cdf_auth_readwrite_all.variables) == 2

    def test_deploy_module(
        self, cognite_toolkit: CogniteToolkit, cognite_client_approval: ApprovalCogniteClient
    ) -> None:
        module = cognite_toolkit.modules.retrieve("cdf_auth_readwrite_all")
        assert isinstance(module, dc.Module)
        module.variables["readwrite_source_id"].value = "123"
        module.variables["readonly_source_id"].value = "456"

        cognite_toolkit.modules.deploy(module)

        dumped = cognite_client_approval.dump(sort=False)
        groups = dumped["groups"]

        assert "cdf_auth_readwrite_all" in groups
        assert groups["cdf_auth_readwrite_all"]["sourceId"] == "123"
        assert "cdf_auth_readonly_all" in groups
        assert groups["cdf_auth_readonly_all"]["sourceId"] == "456"
