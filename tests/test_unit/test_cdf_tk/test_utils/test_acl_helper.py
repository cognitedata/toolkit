from dataclasses import dataclass

from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataSetScope
from cognite_toolkit._cdf_tk.constants import DRY_RUN_ID
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource


@dataclass
class _Item:
    data_set_id: int | None


class TestDatasetScopedResource:
    def test_dry_run_placeholder_uses_all_scope(self) -> None:
        scope = dataset_scoped_resource([_Item(data_set_id=DRY_RUN_ID)])
        assert isinstance(scope, AllScope)

    def test_mixes_known_and_dry_run_ids(self) -> None:
        scope = dataset_scoped_resource([_Item(data_set_id=DRY_RUN_ID), _Item(data_set_id=42)])
        assert isinstance(scope, DataSetScope)
        assert scope.ids == [42]

    def test_known_ids_sorted(self) -> None:
        scope = dataset_scoped_resource([_Item(data_set_id=3), _Item(data_set_id=1)])
        assert isinstance(scope, DataSetScope)
        assert scope.ids == [1, 3]
