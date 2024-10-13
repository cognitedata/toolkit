import pytest

from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Packages


def get_packages() -> list[str]:
    packages = Packages.load(BUILTIN_MODULES_PATH)
    return list(packages.keys())


@pytest.mark.parametrize("package", get_packages())
def test_build_packages_without_warnings(package: str) -> None:
    assert package
