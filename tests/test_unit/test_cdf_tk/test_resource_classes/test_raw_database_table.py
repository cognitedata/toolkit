import pytest

from cognite_toolkit._cdf_tk.resource_classes import DatabaseYAML, TableYAML
from tests.test_unit.utils import find_resources


class TestRawDatabaseTableYAML:
    @pytest.mark.parametrize("data", list(find_resources("Database")))
    def test_load_valid_database(self, data: dict[str, object]) -> None:
        loaded = DatabaseYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(find_resources("Table")))
    def test_load_valid_table(self, data: dict[str, object]) -> None:
        loaded = TableYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
