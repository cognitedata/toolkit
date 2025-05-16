import pytest

from cognite_toolkit._cdf_tk.resource_classes.location import LocationYAML
from tests.test_unit.utils import find_resources


class TestLocationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Location")))
    def test_load_valid_location(self, data: dict[str, object]) -> None:
        loaded = LocationYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
