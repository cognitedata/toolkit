import pytest

from cognite_toolkit._cdf_tk.rules import NeatRuleSet

pytest.importorskip("cognite.neat")

from cognite.neat._data_model.models.dms._container import ContainerRequest
from cognite.neat._data_model.models.dms._data_model import DataModelRequest
from cognite.neat._data_model.models.dms._schema import RequestSchema
from cognite.neat._data_model.models.dms._views import ViewRequest


class TestApplyToolkitGovernedSpaces:
    def test_collects_spaces_from_schema_resources(self) -> None:
        schema = RequestSchema(
            dataModel=DataModelRequest(space="dm_space", externalId="MyModel", version="1"),
            containers=[
                ContainerRequest(space="records_space", externalId="Record", properties={}),
            ],
            views=[
                ViewRequest(space="view_space", externalId="MyView", version="1", properties={}),
            ],
        )

        NeatRuleSet._apply_all_schema_spaces_as_governed_spaces(schema)

        assert schema.governed_space_set() == {"dm_space", "records_space", "view_space"}
