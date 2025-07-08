from datetime import datetime

import pytest
from cognite.client.data_classes import (
    Asset,
    CountAggregate,
    DataSet,
    UserProfile,
    UserProfileList,
)
from cognite.client.data_classes.data_modeling import NodeList
from questionary import Choice

from cognite_toolkit._cdf_tk.client.data_classes.canvas import CANVAS_INSTANCE_SPACE, Canvas
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    InteractiveCanvasSelect,
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

            selector = AssetInteractiveSelect(client, "test_operation")
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
            selector = FileMetadataInteractiveSelect(client, "test_operation")
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
            selector = FileMetadataInteractiveSelect(client, "test_operation")
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
            selector = TimeSeriesInteractiveSelect(client, "test_operation")
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
            selector = EventInteractiveSelect(client, "test_operation")
            selected_hierarchy, selected_dataset = selector.interactive_select_hierarchy_datasets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]


class TestInteractiveCanvasSelect:
    @pytest.mark.parametrize(
        "selected_cdf, answers, expected_selected, expected_options",
        [
            pytest.param({}, ["All Canvases"], [], None, id="No canvases in CDF"),
            pytest.param(
                {"Public1", "Public2", "Private1", "Private2"},
                ["All Canvases"],
                ["Public1", "Public2", "Private1", "Private2"],
                None,
                id="All canvases selected",
            ),
            pytest.param(
                {"Public1", "Public2"},
                ["All public Canvases"],
                ["Public1", "Public2"],
                None,
                id="All public canvases selected",
            ),
            pytest.param(
                {"Public1", "Public2"},
                ["Selected public Canvases", {"Public1"}],
                ["Public1"],
                2,
                id="Selected public canvases",
            ),
            pytest.param(
                {"Public1", "Public2", "Private1", "Private2"},
                ["All by given user", "Marge Simpson (marge)"],
                ["Public2", "Private1"],
                None,
                id="All by given user",
            ),
            pytest.param(
                {"Public1", "Public2", "Private1", "Private2"},
                ["Selected by given user", "Marge Simpson (marge)", {"Public2"}],
                ["Public2"],
                2,
                id="Selected by given user",
            ),
        ],
    )
    def test_interactive_selection(
        self,
        selected_cdf: set[str],
        answers: list,
        expected_selected: list[str],
        expected_options: int | None,
        monkeypatch,
    ) -> None:
        default_args = dict(
            space=CANVAS_INSTANCE_SPACE,
            version=1,
            last_updated_time=1,
            created_time=1,
            updated_by="Irrelevant",
            updated_at=datetime.now(),
        )
        cdf_canvases = [
            Canvas(external_id="Public1", name="Canvas 1", visibility="public", created_by="homer", **default_args),
            Canvas(external_id="Public2", name="Canvas 2", visibility="public", created_by="marge", **default_args),
            Canvas(external_id="Private1", name="Private 1", visibility="private", created_by="marge", **default_args),
            Canvas(external_id="Private2", name="Private 2", visibility="private", created_by="homer", **default_args),
        ]
        first_answer_by_choice_title = {c.title: c.value for c in InteractiveCanvasSelect.opening_choices}
        assert len(answers) >= 1, "At least one answer is required to select a canvas"
        assert answers[0] in first_answer_by_choice_title, "Bug in test data. First answer must be a choice title"
        if "user" in answers[0] and len(answers) >= 2:
            user_title = answers[1]

            def select_user(choices: list[Choice]) -> str:
                assert len(choices) == 2
                user_choice = next((c for c in choices if c.title == user_title), None)
                assert user_choice is not None, f"Bug in test data. User choice '{user_title}' not found in choices"
                return user_choice.value

            answers[1] = select_user
        answers[0] = first_answer_by_choice_title[answers[0]]

        if expected_options is not None:
            # Last question is which canvases to select.
            last_selection = answers[-1]

            def select_canvases(choices: list[Choice]) -> list[str]:
                assert len(choices) == expected_options, (
                    f"Expected {expected_options} choices, but got {len(choices)} choices: {choices}"
                )
                return [choice.value for choice in choices if choice.value in last_selection]

            answers[-1] = select_canvases

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(InteractiveCanvasSelect.__module__, monkeypatch, answers),
        ):
            client.canvas.list.return_value = NodeList[Canvas](
                [canvas for canvas in cdf_canvases if canvas.external_id in selected_cdf]
            )
            client.iam.user_profiles.list.return_value = UserProfileList(
                [
                    UserProfile(user_identifier="homer", display_name="Homer Simpson", last_updated_time=1),
                    UserProfile(user_identifier="marge", display_name="Marge Simpson", last_updated_time=1),
                ]
            )
            selector = InteractiveCanvasSelect(client)
            selected_external_ids = selector.select_external_ids()

        assert selected_external_ids == expected_selected

    def test_interactive_abort_selection(self, monkeypatch) -> None:
        answers = [None]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(InteractiveCanvasSelect.__module__, monkeypatch, answers),
        ):
            selector = InteractiveCanvasSelect(client)
            with pytest.raises(ToolkitValueError) as exc_info:
                selector.select_external_ids()
        assert str(exc_info.value) == "No Canvas selection made. Aborting."
