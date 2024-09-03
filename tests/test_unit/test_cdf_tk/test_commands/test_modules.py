from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.data_classes._packages import Packages, SelectableModule
from tests.data import PACKAGE_FOR_TEST


@pytest.fixture(scope="session")
def selected_packages() -> dict[str, list[SelectableModule]]:
    available = Packages.load(PACKAGE_FOR_TEST)
    return {"quickstart": available["quickstart"]}


class TestModulesCommand:
    def test_modules_command(self, selected_packages: dict[str, list[SelectableModule]]) -> None:
        assert selected_packages is not None
        pass
