import pytest

from cognite_toolkit._cdf_tk.resource_classes.labels import LabelsYAML
from tests.test_unit.utils import find_resources


class TestLabelsYAML:
    @pytest.mark.parametrize("data", list(find_resources("Label")))
    def test_load_valid_label(self, data: dict[str, object]) -> None:
        loaded = LabelsYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
