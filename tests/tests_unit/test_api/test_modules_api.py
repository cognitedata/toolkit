from cognite_toolkit import CogniteToolkit
from cognite_toolkit import data_classes as dc
from cognite_toolkit._cdf_tk.templates import COGNITE_MODULES, iterate_modules
from tests.constants import REPO_ROOT


class TestModulesAPI:
    def test_list_modules(self, cognite_toolkit: CogniteToolkit):
        expected_modules = {
            modul_name for modul_name, _ in iterate_modules(REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES)
        }

        modules = cognite_toolkit.modules.list()

        assert isinstance(modules, dc.ModuleList)
        assert set(modules.names) == expected_modules
