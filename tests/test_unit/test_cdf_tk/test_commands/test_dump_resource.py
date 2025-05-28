from _pytest.monkeypatch import MonkeyPatch
from cognite.client import data_modeling as dm
from questionary import Choice

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilter, LocationFilterList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder, LocationFilterFinder
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
            dm.DataModel("my_space2", "second_model", "v1", **default_args),
        ]
        toolkit_client_approval.append(dm.DataModel, models)
        selected = models[1].as_id()
        finder = DataModelFinder(toolkit_client_approval.mock_client, None)
        answers = ["my_space2", selected, False]
        with MockQuestionary(DataModelFinder.__module__, monkeypatch, answers):
            result = finder._interactive_select()

        assert result == selected
        assert finder.data_model.as_id() == selected


class TestLocationFilterFinder:
    def test_select_location_filter(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ) -> None:
        def select_filters(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[1].value, choices[2].value]

        answers = [select_filters]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(LocationFilterFinder.__module__, monkeypatch, answers),
        ):
            client.location_filters.list.return_value = LocationFilterList(
                [
                    LocationFilter(1, external_id="filterA", name="Filter A", created_time=1, updated_time=1),
                    LocationFilter(2, external_id="filterB", name="Filter B", created_time=1, updated_time=1),
                    LocationFilter(3, external_id="filterC", name="Filter C", created_time=1, updated_time=1),
                ]
            )
            finder = LocationFilterFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("filterB", "filterC")
