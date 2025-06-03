import pytest

from cognite_toolkit._cdf_tk.resource_classes import SpaceYAML
from tests.test_unit.utils import find_resources


class TestSpaceYAML:
    @pytest.mark.parametrize("data", list(find_resources("Space")))
    def test_load_valid_space(self, data: dict[str, object]) -> None:
        loaded = SpaceYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
