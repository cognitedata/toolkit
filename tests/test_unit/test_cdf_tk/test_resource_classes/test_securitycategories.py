import pytest

from cognite_toolkit._cdf_tk.resource_classes import SecurityCategoriesYAML
from tests.test_unit.utils import find_resources


class TestSecuritytCategoriesYAML:
    @pytest.mark.parametrize("data", list(find_resources("SecurityCategory")))
    def test_load_valid_security_categories(self, data: dict[str, object]) -> None:
        loaded = SecurityCategoriesYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
