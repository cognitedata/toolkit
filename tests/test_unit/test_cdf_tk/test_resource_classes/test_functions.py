import pytest

from cognite_toolkit._cdf_tk.resource_classes.functions import FunctionsYAML
from tests.test_unit.utils import find_resources


class TestFunctionsYAML:
    @pytest.mark.parametrize("data", list(find_resources("function")))
    def test_load_valid_function(self, data: dict[str, object]) -> None:
        loaded = FunctionsYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
