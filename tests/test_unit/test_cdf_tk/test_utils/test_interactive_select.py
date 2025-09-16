from collections.abc import Mapping
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import (
    Asset,
    CountAggregate,
    DataSet,
    UserProfile,
    UserProfileList,
)
from cognite.client.data_classes.aggregations import CountValue
from cognite.client.data_classes.data_modeling import NodeList, Space, SpaceList, View, ViewId, ViewList
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics, SpaceStatisticsList
from cognite.client.data_classes.raw import Database, DatabaseList, Table, TableList
from questionary import Choice

from cognite_toolkit._cdf_tk.client.data_classes.canvas import CANVAS_INSTANCE_SPACE, Canvas
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import ChartData
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils.aggregators import AssetCentricAggregator
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetCentricDestinationSelect,
    AssetInteractiveSelect,
    DataModelingSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    InteractiveCanvasSelect,
    InteractiveChartSelect,
    RawTableInteractiveSelect,
    TimeSeriesInteractiveSelect,
)
from tests.test_unit.utils import MockQuestionary


class TestInteractiveSelect:
    def test_interactive_select_assets(self, monkeypatch) -> None:
        def select_hierarchy(choices: list[Choice]) -> str:
            assert len(choices) == 2
            return choices[1].value

        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3 + 1  # +1 for "All Data Sets" option
            return [choices[3].value]

        answers = ["Hierarchy", select_hierarchy, select_data_set]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test_operation")
            client.assets.list.return_value = [Asset(id=1, external_id="Root1"), Asset(id=2, external_id="Root2")]
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            aggregator.used_data_sets.return_value = ["dataset1", "dataset2", "dataset3"]
            selector._aggregator = aggregator
            client.data_sets.retrieve_multiple.return_value = [
                DataSet(id=1, external_id="dataset1"),
                DataSet(id=2, external_id="dataset2"),
                DataSet(id=3, external_id="dataset3"),
            ]

            selected_hierarchy, selected_dataset = selector.select_hierarchies_and_data_sets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_filemetadata(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> str:
            assert len(choices) == 3
            return choices[2].value

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2 + 1  # +1 for "All Hierarchies" option
            return [choices[2].value]

        answers = ["Data Set", select_data_set, select_hierarchy]
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
            selected_hierarchy, selected_dataset = selector.select_hierarchies_and_data_sets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_filemetadata_empty_cdf(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> None:
            assert len(choices) == 0
            return None

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 0
            return []

        answers = ["Data Set", select_data_set, select_hierarchy]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(FileMetadataInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = []
            client.assets.list.return_value = []
            client.files.aggregate.return_value = [CountAggregate(100)]
            selector = FileMetadataInteractiveSelect(client, "test_operation")
            with pytest.raises(ToolkitValueError) as exc_info:
                _ = selector.select_hierarchies_and_data_sets()

        assert str(exc_info.value) == "No data Set available to select."

    def test_interactive_select_timeseries(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> str:
            assert len(choices) == 3
            return choices[2].value

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2 + 1  # +1 for "All Hierarchies" option
            return [choices[2].value]

        answers = ["Data Set", select_data_set, select_hierarchy]
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
            selected_hierarchy, selected_dataset = selector.select_hierarchies_and_data_sets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_interactive_select_events(self, monkeypatch) -> None:
        def select_data_set(choices: list[Choice]) -> str:
            assert len(choices) == 3
            return choices[2].value

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2 + 1  # +1 for "All Hierarchies" option
            return [choices[2].value]

        answers = ["Data Set", select_data_set, select_hierarchy]
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
            selected_hierarchy, selected_dataset = selector.select_hierarchies_and_data_sets()

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_asset_centric_destination_select(self, monkeypatch) -> None:
        def select_destination(choices: list[Choice]) -> str:
            assert len(choices) == len(AssetCentricDestinationSelect.valid_destinations)
            return choices[2].value

        answers = [select_destination]
        with MockQuestionary(AssetCentricDestinationSelect.__module__, monkeypatch, answers):
            selected = AssetCentricDestinationSelect.select()

        assert selected == AssetCentricDestinationSelect.valid_destinations[2]

    def test_asset_centric_destination_invalid_destination(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            AssetCentricDestinationSelect.validate("invalid_destination")

        assert "Invalid destination type: 'invalid_destination'." in str(exc_info.value)

    def test_select_data_set(self, monkeypatch):
        def select_data_set(choices) -> str:
            assert len(choices) == 2
            selection = choices[1].value
            assert isinstance(selection, str)
            return selection

        answers = [select_data_set]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator

            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="ds1", name="DataSet 1"),
                DataSet(id=2, external_id="ds2", name="DataSet 2"),
            ]
            result = selector.select_data_set()
        assert result == "ds2"

    def test_select_data_sets_allow_empty(self, monkeypatch) -> None:
        def select_data_sets(choices) -> str | None:
            assert len(choices) == 2 + 1  # +1 for "All Data Sets" option
            assert choices[0].title.startswith("All")
            return choices[0].value

        answers = [select_data_sets]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator

            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="ds1", name="DataSet 1"),
                DataSet(id=2, external_id="ds2", name="DataSet 2"),
            ]

            result = selector.select_data_set(allow_empty=True)
        assert result is None

    def test_select_data_sets(self, monkeypatch):
        def select_data_sets(choices) -> str:
            assert len(choices) == 2
            selection = choices[0].value
            assert isinstance(selection, str)
            return selection

        answers = [select_data_sets]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator

            client.data_sets.list.return_value = [
                DataSet(id=1, external_id="ds1", name="DataSet 1"),
                DataSet(id=2, external_id="ds2", name="DataSet 2"),
            ]

            result = selector.select_data_sets()
        assert result == "ds1"

    def test_select_hierarchy(self, monkeypatch):
        def select_hierarchy(choices) -> str:
            assert len(choices) == 2
            selection = choices[0].value
            assert isinstance(selection, str)
            return selection

        answers = [select_hierarchy]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator

            client.assets.list.return_value = [
                Asset(id=1, external_id="root1", name="Root 1"),
                Asset(id=2, external_id="root2", name="Root 2"),
            ]

            result = selector.select_hierarchy()
        assert result == "root1"

    def test_select_hierarchy_allow_empty(self, monkeypatch) -> None:
        def select_hierarchy(choices) -> str | None:
            assert len(choices) == 2 + 1
            # +1 for "All Hierarchies" option
            assert choices[0].title.startswith("All")
            return choices[0].value

        answers = [select_hierarchy]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator

            client.assets.list.return_value = [
                Asset(id=1, external_id="root1", name="Root 1"),
                Asset(id=2, external_id="root2", name="Root 2"),
            ]

            result = selector.select_hierarchy(allow_empty=True)
        assert result is None

    def test_select_hierarchies(self, monkeypatch):
        def select_hierarchies(choices) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        answers = [select_hierarchies]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AssetInteractiveSelect.__module__, monkeypatch, answers),
        ):
            selector = AssetInteractiveSelect(client, "test")
            aggregator = MagicMock(spec=AssetCentricAggregator)
            aggregator.count.return_value = 1000
            selector._aggregator = aggregator
            client.assets.list.return_value = [
                Asset(id=1, external_id="root1", name="Root 1"),
                Asset(id=2, external_id="root2", name="Root 2"),
            ]
            result = selector.select_hierarchies()
        assert result == ["root2"]


class TestRawTableSelect:
    def test_interactive_select_raw_table(self, monkeypatch) -> None:
        def select_database(choices: list[Choice]) -> str:
            assert len(choices) == 3
            return choices[2].value

        def select_tables(choices: list[Choice]) -> list[RawTable]:
            assert len(choices) == 23
            return [choices[i].value for i in range(0, 24, 2)]

        answers = [select_database, select_tables]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(RawTableInteractiveSelect.__module__, monkeypatch, answers),
        ):
            client.raw.databases.list.return_value = DatabaseList([Database(name=f"Database{i}") for i in range(1, 4)])
            client.raw.tables.list.return_value = TableList([Table(name=f"Table{i}") for i in range(1, 24)])
            selected = RawTableInteractiveSelect(client, "test_operation").select_tables()

        assert selected == [RawTable("Database3", f"Table{i}") for i in range(1, 24, 2)]


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


class TestInteractiveChartSelect:
    @pytest.mark.parametrize(
        "selected_cdf, answers, expected_selected, expected_options",
        [
            pytest.param([], ["All public Charts"], [], None, id="No charts in CDF"),
            pytest.param(
                ["homer1", "homer2", "marge1", "marge2"],
                ["All public Charts"],
                ["homer1", "homer2", "marge1", "marge2"],
                None,
                id="All public charts selected",
            ),
            pytest.param(
                ["homer1", "homer2", "marge1", "marge2"],
                ["Selected public Charts", {"homer1"}],
                ["homer1"],
                4,
                id="Selected public charts",
            ),
            pytest.param(
                ["homer1", "homer2", "marge1", "marge2"],
                ["All owned by given user", "Marge Simpson (marge)"],
                ["marge1", "marge2"],
                None,
                id="All by given user",
            ),
            pytest.param(
                ["homer1", "homer2", "marge1", "marge2"],
                ["Selected owned by given user", "Marge Simpson (marge)", {"marge2"}],
                ["marge2"],
                2,
                id="Selected by given user",
            ),
        ],
    )
    def test_interactive_selection(
        self,
        selected_cdf: list[str],
        answers: list,
        expected_selected: list[str],
        expected_options: int | None,
        monkeypatch,
    ) -> None:
        # Use real Chart and ChartData objects instead of DummyChart
        default_args = dict(
            created_time=1,
            last_updated_time=1,
            visibility="PUBLIC",
        )
        cdf_charts = [
            Chart(external_id="homer1", data=ChartData(name="Homer 1"), owner_id="homer", **default_args),
            Chart(external_id="homer2", data=ChartData(name="Homer 2"), owner_id="homer", **default_args),
            Chart(external_id="marge1", data=ChartData(name="Marge 1"), owner_id="marge", **default_args),
            Chart(external_id="marge2", data=ChartData(name="Marge 2"), owner_id="marge", **default_args),
        ]
        # Map answer titles to opening_choices values
        first_answer_by_choice_title = {c.title: c.value for c in InteractiveChartSelect.opening_choices}
        assert len(answers) >= 1, "At least one answer is required to select a chart"
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
            last_selection = answers[-1]

            def select_charts(choices: list[Choice]) -> list[str]:
                assert len(choices) == expected_options, (
                    f"Expected {expected_options} choices, but got {len(choices)} choices: {choices}"
                )
                return [choice.value for choice in choices if choice.value in last_selection]

            answers[-1] = select_charts
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(InteractiveChartSelect.__module__, monkeypatch, answers),
        ):
            # Only include charts whose external_id is in selected_cdf
            client.charts.list.return_value = ChartList(
                [chart for chart in cdf_charts if chart.external_id in selected_cdf]
            )
            client.iam.user_profiles.list.return_value = UserProfileList(
                [
                    UserProfile(user_identifier="homer", display_name="Homer Simpson", last_updated_time=1),
                    UserProfile(user_identifier="marge", display_name="Marge Simpson", last_updated_time=1),
                ]
            )
            selector = InteractiveChartSelect(client)
            selected_external_ids = selector.select_external_ids()
        assert selected_external_ids == expected_selected

    def test_interactive_abort_selection(self, monkeypatch) -> None:
        answers = [None]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(InteractiveChartSelect.__module__, monkeypatch, answers),
        ):
            selector = InteractiveChartSelect(client)
            with pytest.raises(ToolkitValueError) as exc_info:
                selector.select_external_ids()
        assert str(exc_info.value) == "No Chart selection made. Aborting."


class TestDataModelingInteractiveSelect:
    DEFAULT_SPACE_ARGS: Mapping = dict(
        description="Test Space",
        name="Test Space",
        created_time=1,
        last_updated_time=1,
        is_global=False,
    )
    DEFAULT_VIEW_ARGS: Mapping = dict(
        properties={},
        last_updated_time=1,
        created_time=1,
        description=None,
        name=None,
        filter=None,
        implements=None,
        writable=True,
        used_for="node",
        is_global=False,
    )

    def test_select_view(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]
        views = [
            View(space="space1", external_id="view1", version="1", **self.DEFAULT_VIEW_ARGS),
            View(space="space1", external_id="view2", version="1", **self.DEFAULT_VIEW_ARGS),
        ]
        space_stats = SpaceStatisticsList([SpaceStatistics(space.space, 0, 1, 0, 0, 0, 0, 0) for space in spaces])

        answers = [spaces[0], views[1]]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.views.list.return_value = ViewList(views)
            client.data_modeling.statistics.spaces.list.return_value = space_stats
            selector = DataModelingSelect(client, "test_operation")
            selected_view = selector.select_view()

        assert selected_view.external_id == "view2"

    def test_select_no_schema_space_found(self, monkeypatch) -> None:
        space = Space(space="space1", **self.DEFAULT_SPACE_ARGS)
        space_stats = [SpaceStatistics(space.space, 0, 0, 0, 0, 0, 0, 0)]
        answers = [space]  # Direct string answer
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList([space])
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)
            client.data_modeling.views.list.return_value = []
            selector = DataModelingSelect(client, "test_operation")
            with pytest.raises(ToolkitMissingResourceError) as exc_info:
                selector.select_view()
            assert str(exc_info.value) == "No spaces with schema (containers, views, or data models) found."

    def test_select_instance_type(self, monkeypatch) -> None:
        answers = ["node"]  # Direct string answer
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            selector = DataModelingSelect(client, "test_operation")
            instance_type = selector.select_instance_type("all")

        assert instance_type == "node"

    def test_select_single_schema_space(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]
        stats = [SpaceStatistics(space.space, 0, 1, 0, 0, 0, 0, 0) for space in spaces]
        answers = [spaces[1]]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(stats)
            selector = DataModelingSelect(client, "test_operation")
            selected_space = selector.select_schema_space(include_global=True)

        assert selected_space.space == "space2"

    def test_select_instance_spaces_one_space_with_instances(self, monkeypatch) -> None:
        def mock_aggregate(view_id, count, instance_type, space):
            if space == "space1":
                return CountValue("externalId", 5)
            return CountValue("externalId", 0)

        with monkeypatch_toolkit_client() as client:
            client.data_modeling.spaces.list.return_value = SpaceList(
                [
                    Space(space="space1", **self.DEFAULT_SPACE_ARGS),
                    Space(space="space2", **self.DEFAULT_SPACE_ARGS),
                ]
            )
            client.data_modeling.instances.aggregate.side_effect = mock_aggregate
            client.data_modeling.statistics.project().concurrent_read_limit = 2

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_instance_space(True, ViewId("space1", "view1", "1"), "node")

        assert selected_spaces == ["space1"]

    def test_select_instance_spaces_multiple_spaces(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
            Space(space="space3", **self.DEFAULT_SPACE_ARGS),
        ]

        def select_space(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[0].value, choices[2].value]

        answers = [select_space]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.instances.aggregate.return_value = CountValue("externalId", 5)
            client.data_modeling.statistics.project().concurrent_read_limit = 6

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_instance_space(True, ViewId("space1", "view1", "1"), "node")

        assert selected_spaces == ["space1", "space3"]

    def test_select_instance_spaces_no_instances(self, monkeypatch) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.spaces.list.return_value = SpaceList(
                [
                    Space(space="space1", **self.DEFAULT_SPACE_ARGS),
                    Space(space="space2", **self.DEFAULT_SPACE_ARGS),
                ]
            )
            client.data_modeling.instances.aggregate.return_value = CountValue("externalId", 0)
            client.data_modeling.statistics.project().concurrent_read_limit = 2

            selector = DataModelingSelect(client, "test_operation")
            with pytest.raises(ToolkitMissingResourceError) as exc_info:
                selector.select_instance_space(True, ViewId("space1", "view1", "1"), "node")

            assert str(exc_info.value) == (
                "No instances found in any space for the view "
                "ViewId(space='space1', external_id='view1', version='1') with instance type 'node'."
            )

    def test_select_space_type(self, monkeypatch) -> None:
        answers = ["schema"]  # Direct string answer
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            selector = DataModelingSelect(client, "test_operation")
            space_type = selector.select_space_type()

        assert space_type == "schema"

    def test_select_empty_spaces(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
            Space(space="space3", **self.DEFAULT_SPACE_ARGS),
        ]

        # Set up space statistics to make spaces 1 and 3 empty
        space_stats = [
            SpaceStatistics("space1", 0, 0, 0, 0, 0, 0, 0),  # Empty space
            SpaceStatistics("space2", 5, 2, 1, 0, 0, 0, 0),  # Non-empty space
            SpaceStatistics("space3", 0, 0, 0, 0, 0, 0, 0),  # Empty space
        ]

        def select_spaces(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2  # Only the empty spaces should be presented
            return [choices[1].value]  # Select space3

        answers = [select_spaces]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_empty_spaces()

        assert selected_spaces == ["space3"]

    def test_select_empty_spaces_single_space(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]

        # Only space1 is empty
        space_stats = [
            SpaceStatistics("space1", 0, 0, 0, 0, 0, 0, 0),  # Empty space
            SpaceStatistics("space2", 5, 2, 1, 0, 0, 0, 0),  # Non-empty space
        ]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, []),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_empty_spaces()

        assert selected_spaces == ["space1"]

    def test_select_empty_spaces_no_spaces(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]

        # No empty spaces
        space_stats = [
            SpaceStatistics("space1", 5, 0, 0, 0, 0, 0, 0),
            SpaceStatistics("space2", 0, 2, 1, 0, 0, 0, 0),
        ]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, []),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            with pytest.raises(ToolkitMissingResourceError) as exc_info:
                selector.select_empty_spaces()

            assert str(exc_info.value) == "No empty spaces found."

    def test_select_instance_spaces(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
            Space(space="space3", **self.DEFAULT_SPACE_ARGS),
        ]

        # Set up space statistics with different instance counts
        space_stats = [
            SpaceStatistics("space1", 0, 0, 0, 0, 0, 10, 0),  # 10 nodes
            SpaceStatistics("space2", 0, 0, 0, 0, 0, 0, 0),  # No instances
            SpaceStatistics("space3", 0, 0, 0, 5, 0, 5, 0),  # 5 nodes, 5 edges
        ]

        def select_spaces(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2  # Only spaces with instances should be presented
            return [choices[1].value]  # Select space3

        answers = [select_spaces]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, answers),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_instance_space()

        assert selected_spaces == ["space3"]

    def test_select_instance_spaces_without_view_or_instance_type_single_space(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]

        # Only space1 has instances
        space_stats = [
            SpaceStatistics("space1", 0, 0, 0, 5, 0, 2, 0),  # Has instances
            SpaceStatistics("space2", 0, 0, 0, 0, 0, 0, 0),  # No instances
        ]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, []),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            selected_spaces = selector.select_instance_space()

        assert selected_spaces == ["space1"]

    def test_select_instance_spaces_without_view_or_instance_type_no_instances(self, monkeypatch) -> None:
        spaces = [
            Space(space="space1", **self.DEFAULT_SPACE_ARGS),
            Space(space="space2", **self.DEFAULT_SPACE_ARGS),
        ]

        # No spaces have instances
        space_stats = [
            SpaceStatistics("space1", 0, 0, 1, 0, 0, 0, 0),  # Only has containers
            SpaceStatistics("space2", 0, 2, 0, 0, 0, 0, 0),  # Only has views
        ]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataModelingSelect.__module__, monkeypatch, []),
        ):
            client.data_modeling.spaces.list.return_value = SpaceList(spaces)
            client.data_modeling.statistics.spaces.list.return_value = SpaceStatisticsList(space_stats)

            selector = DataModelingSelect(client, "test_operation")
            with pytest.raises(ToolkitMissingResourceError) as exc_info:
                selector.select_instance_space()

            assert "No instances found in any space" in str(exc_info.value)
