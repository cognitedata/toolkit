from _pytest.monkeypatch import MonkeyPatch
from cognite.client import data_modeling as dm

from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import MockQuestionary


class TestDataModelFinder:
    def test_select_data_model(self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch) -> None:
        default_args = dict(
            is_global=False,
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            views=[],
        )
        models = [
            dm.DataModel("my_space", "first_model", "v1", **default_args),
            dm.DataModel("my_space", "second_model", "v1", **default_args),
        ]
        toolkit_client_approval.append(dm.DataModel, models)
        toolkit_client_approval.append(dm.Space, dm.Space("my_space", False, created_time=1, last_updated_time=1))
        selected = models[1].as_id()
        finder = DataModelFinder(toolkit_client_approval.mock_client, None)
        answers = [
            "my_space",
            selected,
            False,
        ]
        with MockQuestionary(DataModelFinder.__module__, monkeypatch, answers):
            result = finder._interactive_select()

        assert result == selected
        assert finder.data_model.as_id() == selected
