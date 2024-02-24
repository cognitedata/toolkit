from cognite_toolkit import CogniteToolkit
from cognite_toolkit import data_classes as dc
from cognite_toolkit._cdf_tk.templates import COGNITE_MODULES, iterate_modules
from tests.constants import REPO_ROOT


class TestModulesAPI:
    def test_list_modules(self, cognite_toolkit: CogniteToolkit):
        expected_modules = {
            modul_path.name for modul_path, _ in iterate_modules(REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES)
        }

        modules = cognite_toolkit.modules.list()

        assert isinstance(modules, dc.ModuleList)
        assert set(modules.names) == expected_modules

        cdf_auth_readwrite_all = modules["cdf_auth_readwrite_all"]
        assert cdf_auth_readwrite_all.name == "cdf_auth_readwrite_all"
        assert len(cdf_auth_readwrite_all.variables) == 2
