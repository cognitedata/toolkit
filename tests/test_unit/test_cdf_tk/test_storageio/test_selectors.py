from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any, get_args

import pytest

from cognite_toolkit._cdf_tk.storageio.selectors import (
    AllChartSelector,
    AssetSubtreeSelector,
    ChartOwnerSelector,
    DataSelector,
    DataSetSelector,
    InstanceFileSelector,
    InstanceViewSelector,
    RawTableSelector,
    Selector,
    SelectorAdapter,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file


def example_selector_data() -> Iterable[tuple]:
    yield pytest.param(
        {"type": "rawTable", "table": {"dbName": "my_db", "tableName": "my_table"}},
        RawTableSelector,
        id="RawTableSelector",
    )
    yield pytest.param(
        {
            "type": "instanceView",
            "view": {"space": "my_space", "externalId": "my_view", "version": "v1"},
            "instanceType": "node",
            "instanceSpaces": ["space1", "space2"],
        },
        InstanceViewSelector,
        id="InstanceViewSelector",
    )
    yield pytest.param(
        {"type": "instanceFile", "datafile": "path/to/file.csv", "validateInstance": True},
        InstanceFileSelector,
        id="InstanceFileSelector",
    )
    yield pytest.param(
        {"type": "dataSet", "dataSetExternalId": "my_data_set", "resourceType": "asset"},
        DataSetSelector,
        id="DataSetSelector",
    )
    yield pytest.param(
        {"type": "assetSubtree", "hierarchy": "root/child", "resourceType": "asset"},
        AssetSubtreeSelector,
        id="AssetSubtreeSelector",
    )
    yield pytest.param(
        {"type": "chartOwner", "ownerId": "doctrino"},
        ChartOwnerSelector,
        id="ChartOwnerSelector",
    )
    yield pytest.param(
        {"type": "allCharts"},
        AllChartSelector,
        id="AllChartSelector",
    )


class TestDataSelectors:
    """Test to ensure all data selectors are working as expected."""

    def test_all_selectors_in_union(self) -> None:
        all_selectors = get_concrete_subclasses(DataSelector)
        all_union_selectors = get_args(Selector.__args__[0])
        missing = set(all_selectors) - set(all_union_selectors)
        assert not missing, (
            f"The following DataSelector subclasses are "
            f"missing from the Selector union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_all_types_are_unique(self) -> None:
        all_selectors = get_concrete_subclasses(DataSelector)
        types = Counter(cls.model_fields["type"].default for cls in all_selectors)
        duplicates = [t for t, count in types.items() if count > 1]
        assert not duplicates, f"The following DataSelector types are not unique: {humanize_collection(duplicates)}"

    def test_example_data_is_complete(self) -> None:
        all_selectors = get_concrete_subclasses(DataSelector)
        example_types = {p.values[0]["type"] for p in example_selector_data()}
        all_types = {cls.model_fields["type"].default for cls in all_selectors}
        missing = all_types - example_types
        assert not missing, f"The following DataSelector types are missing example data: {humanize_collection(missing)}"

    @pytest.mark.parametrize("data,expected_selector", list(example_selector_data()))
    def test_selector_instance(
        self, data: dict[str, Any], expected_selector: type[DataSelector], tmp_path: Path
    ) -> None:
        instance = SelectorAdapter.validate_python(data)

        # Assert correct type
        assert isinstance(instance, expected_selector), (
            f"Expected {expected_selector.__name__}, got {type(instance).__name__}"
        )
        # Assert __str__ is implemented
        assert str(instance), f"__str__ not implemented for {type(instance).__name__}"

        # Assert group is implemented
        assert instance.group, f"group property not implemented for {type(instance).__name__}"

        # Assert serialization/deserialization
        filepath = instance.dump_to_file(tmp_path)
        assert filepath.exists(), f"dump_to_file did not create file for {type(instance).__name__}"

        data = read_yaml_file(filepath)
        loaded = SelectorAdapter.validate_python(data)
        assert loaded.model_dump() == instance.model_dump()
        assert type(loaded) is type(instance)
