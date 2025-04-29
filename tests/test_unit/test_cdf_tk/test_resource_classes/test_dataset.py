import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_classes import DataSetYAML
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from tests.data import COMPLETE_ORG


def find_resources(resource: str):
    base = COMPLETE_ORG / MODULES
    for path in base.rglob(f"*{resource}.yaml"):
        data = read_yaml_file(path)
        if isinstance(data, dict):
            yield pytest.param(data, id=path.relative_to(base).as_posix())
        elif isinstance(data, list):
            for no, item in enumerate(data):
                if isinstance(item, dict):
                    yield pytest.param(item, id=f"{path.relative_to(base).as_posix()} - Item: {no}")
                else:
                    raise ValueError(f"Invalid data format in {path}: {item}")
        else:
            raise ValueError(f"Invalid data format in {path}: {data}")


class TestDataSetYAML:
    @pytest.mark.parametrize("data", list(find_resources("DataSet")))
    def test_load_valid_dataset(self, data: dict[str, object]) -> None:
        loaded = DataSetYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
