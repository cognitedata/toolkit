from cognite.client.data_classes import (
    Asset,
    CountAggregate,
    DataSet,
)
from questionary import Choice

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    TimeSeriesInteractiveSelect,
)
from tests.test_unit.utils import MockQuestionary


class TestInteractiveSelect:
    def test_interactive_select_assets(self, monkeypatch) -> None:
        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[2].value]

        answers = ["Hierarchy", select_hierarchy, "Data Set", select_data_set, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.assets.list.return_value = [Asset(id=1, external_id="Root1"), Asset(id=2, external_id="Root2")]
            client.assets.aggregate_count.return_value = 100
            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="dataset1"),
                DataSet(id=2, external_id="dataset2"),
                DataSet(id=3, external_id="dataset3"),
            ]

            selector = AssetInteractiveSelect(client)
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_filemetadata(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[2].value]

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        answers = ["Data Set", select_data_set, "Hierarchy", select_hierarchy, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(FileMetadataInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="dataset1", name="Dataset 1"),
                DataSet(id=2, external_id="dataset2", name="Dataset 2"),
                DataSet(id=3, external_id="dataset3", name="Dataset 3"),
            ]
            client.assets.list.return_value = [
                Asset(id=1, external_id="Root1", name="Root 1"),
                Asset(id=2, external_id="Root2", name="Root 2"),
            ]
            client.files.aggregate.return_value = [CountAggregate(100)]
            selector = FileMetadataInteractiveSelect(client)
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_filemetadata_empty_cdf(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 0
            return []

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 0
            return []

        answers = ["Data Set", select_data_set, "Hierarchy", select_hierarchy, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(FileMetadataInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = []
            client.assets.list.return_value = []
            client.files.aggregate.return_value = [CountAggregate(100)]
            selector = FileMetadataInteractiveSelect(client)
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == []
        assert selected_dataset == []

    def test_interactive_select_timeseries(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[2].value]

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        answers = ["Data Set", select_data_set, "Hierarchy", select_hierarchy, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(TimeSeriesInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="dataset1", name="Dataset 1"),
                DataSet(id=2, external_id="dataset2", name="Dataset 2"),
                DataSet(id=3, external_id="dataset3", name="Dataset 3"),
            ]
            client.assets.list.return_value = [
                Asset(id=1, external_id="Root1", name="Root 1"),
                Asset(id=2, external_id="Root2", name="Root 2"),
            ]
            client.time_series.aggregate_count.return_value = 100
            selector = TimeSeriesInteractiveSelect(client)
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_events(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[2].value]

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        answers = ["Data Set", select_data_set, "Hierarchy", select_hierarchy, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(EventInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="dataset1", name="Dataset 1"),
                DataSet(id=2, external_id="dataset2", name="Dataset 2"),
                DataSet(id=3, external_id="dataset3", name="Dataset 3"),
            ]
            client.assets.list.return_value = [
                Asset(id=1, external_id="Root1", name="Root 1"),
                Asset(id=2, external_id="Root2", name="Root 2"),
            ]
            client.events.aggregate_count.return_value = 100
            selector = EventInteractiveSelect(client)
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]
