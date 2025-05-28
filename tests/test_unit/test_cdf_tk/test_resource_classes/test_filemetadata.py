import pytest

from cognite_toolkit._cdf_tk.resource_classes import FileMetadataYAML
from tests.test_unit.utils import find_resources


class TestFileMetadataYAML:
    @pytest.mark.parametrize("data", list(find_resources("FileMetadata")))
    def test_load_valid_file_metadata(self, data: dict[str, object]) -> None:
        loaded = FileMetadataYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
