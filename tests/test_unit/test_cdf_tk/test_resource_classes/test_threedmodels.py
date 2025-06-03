import pytest

from cognite_toolkit._cdf_tk.resource_classes import ThreeDModelYAML
from tests.test_unit.utils import find_resources


class TestDataSetYAML:
    @pytest.mark.parametrize("data", list(find_resources("3DModel")))
    def test_load_valid_dataset(self, data: dict[str, object]) -> None:
        loaded = ThreeDModelYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
