import pytest

from cognite_toolkit._cdf_tk.resource_classes.function_schedule import FunctionScheduleYAML
from tests.test_unit.utils import find_resources


class TestFunctionScheduleYAML:
    @pytest.mark.parametrize("data", list(find_resources("schedule", "functions")))
    def test_load_valid_function_schedule(self, data: dict[str, object]) -> None:
        loaded = FunctionScheduleYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
