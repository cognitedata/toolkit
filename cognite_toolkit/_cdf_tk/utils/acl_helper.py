from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.resource_classes.group import ScopeDefinition, DataSetScope, AllScope

class DataSetScopedItem:
    def __init__(self, data_set_id: int | None):
        self.data_set_id = data_set_id

def dataset_scoped_resource(items: Sequence[DataSetScopedItem]) -> ScopeDefinition:
    data_set_ids: set[int] = set()
    for item in items:
        if item.data_set_id is None:
            return AllScope()  # A single unscoped item means we need AllScope.
        data_set_ids.add(item.data_set_id)
    return DataSetScope(ids=list(data_set_ids))
