import json
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    DataSet,
    DataSetList,
    ExtractionPipeline,
    ExtractionPipelineList,
    FileMetadataList,
    Function,
    FunctionList,
    Group,
    GroupList,
    Transformation,
    TransformationList,
    TransformationScheduleList,
)
from cognite.client.data_classes.agents import Agent, AgentList, AskDocumentAgentTool
from cognite.client.data_classes.aggregations import UniqueResult, UniqueResultList
from cognite.client.data_classes.capabilities import (
    TimeSeriesAcl,
)
from cognite.client.data_classes.functions import FunctionsStatus
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.location_filters import LocationFilter, LocationFilterList
from cognite_toolkit._cdf_tk.client.data_classes.streamlit_ import Streamlit, StreamlitList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands.dump_resource import (
    AgentFinder,
    DataModelFinder,
    DataSetFinder,
    DumpResourceCommand,
    ExtractionPipelineFinder,
    FunctionFinder,
    GroupFinder,
    LocationFilterFinder,
    StreamlitFinder,
    TransformationFinder,
)
from cognite_toolkit._cdf_tk.cruds import (
    AgentCRUD,
    DataSetsCRUD,
    ExtractionPipelineCRUD,
    FunctionCRUD,
    GroupAllScopedCRUD,
    LocationFilterCRUD,
    StreamlitCRUD,
    TransformationCRUD,
)
from cognite_toolkit._cdf_tk.utils import read_yaml_file
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import MockQuestionary


@pytest.fixture()
def three_transformations() -> TransformationList:
    return TransformationList(
        [
            Transformation(
                1,
                "transformationA",
                name="My First Transformation",
                ignore_null_fields=True,
                created_time=1,
                updated_time=1,
            ),
            Transformation(
                2,
                "transformationB",
                name="My Second Transformation",
                ignore_null_fields=True,
                created_time=1,
                updated_time=1,
            ),
            Transformation(
                3,
                "transformationC",
                name="My Third Transformation",
                ignore_null_fields=True,
                created_time=1,
                updated_time=1,
            ),
        ]
    )


class TestTransformationFinder:
    def test_select_transformations(self, three_transformations: TransformationList, monkeypatch: MonkeyPatch) -> None:
        def select_transformations(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_transformations)
            return [choices[1].value, choices[2].value]

        answers = [select_transformations]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(TransformationFinder.__module__, monkeypatch, answers),
        ):
            client.transformations.list.return_value = three_transformations
            finder = TransformationFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("transformationB", "transformationC")


class TestDumpTransformations:
    def test_dump_transformations(self, three_transformations: TransformationList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.transformations.retrieve_multiple.return_value = three_transformations[1:]
            client.transformations.schedules.retrieve.return_value = TransformationScheduleList([])

            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                TransformationFinder(client, ("transformationB", "transformationC")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = TransformationCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = [read_yaml_file(filepath) for filepath in filepaths]
        assert items == [loader.dump_resource(t) for t in three_transformations[1:]]


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


@pytest.fixture()
def three_location_filters() -> LocationFilterList:
    return LocationFilterList(
        [
            LocationFilter(1, external_id="filterA", name="Filter A", created_time=1, updated_time=1),
            LocationFilter(2, external_id="filterB", name="Filter B", created_time=1, updated_time=1),
            LocationFilter(3, external_id="filterC", name="Filter C", created_time=1, updated_time=1),
        ]
    )


class TestLocationFilterFinder:
    def test_select_location_filter(self, three_location_filters: LocationFilterList, monkeypatch: MonkeyPatch) -> None:
        def select_filters(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_location_filters)
            return [choices[1].value, choices[2].value]

        answers = [select_filters]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(LocationFilterFinder.__module__, monkeypatch, answers),
        ):
            client.search.locations.list.return_value = three_location_filters
            finder = LocationFilterFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("filterB", "filterC")


class TestDumpLocationFilter:
    def test_dump_location_filter(self, three_location_filters: LocationFilterList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.search.locations.list.return_value = three_location_filters
            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                LocationFilterFinder(client, ("filterB", "filterC")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = LocationFilterCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = [item for filepath in filepaths for item in loader.load_resource_file(filepath)]
        assert items == three_location_filters[1:].as_write().dump()


@pytest.fixture()
def three_agents() -> AgentList:
    return AgentList(
        [
            Agent(
                external_id=f"agent{character}",
                name=f"Agent {character}",
                description=f"This is Agent {character}",
                model="gpt-3.5-turbo",
                tools=[
                    AskDocumentAgentTool(
                        name=f"tool{character}",
                        description=f"This is tool {character}",
                    )
                ],
            )
            for character in ["A", "B", "C"]
        ]
    )


class TestDumpAgents:
    def test_dump_agents(self, three_agents: AgentList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.agents.retrieve.return_value = three_agents[1:]
            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                AgentFinder(client, tuple([agent.external_id for agent in three_agents[1:]])),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = AgentCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = [read_yaml_file(filepath) for filepath in filepaths]
        assert items == [loader.dump_resource(agent) for agent in three_agents[1:]]

    def test_interactive_select_agents(self, three_agents: AgentList, monkeypatch: MonkeyPatch) -> None:
        def select_agents(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_agents)
            return [choices[1].value, choices[2].value]

        answers = [select_agents]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(AgentFinder.__module__, monkeypatch, answers),
        ):
            client.agents.list.return_value = three_agents
            finder = AgentFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("agentB", "agentC")


@pytest.fixture()
def three_extraction_pipelines() -> ExtractionPipelineList:
    return ExtractionPipelineList(
        [
            ExtractionPipeline(
                1, external_id="pipelineA", name="Pipeline A", data_set_id=123, created_time=1, last_updated_time=1
            ),
            ExtractionPipeline(
                2, external_id="pipelineB", name="Pipeline B", data_set_id=123, created_time=1, last_updated_time=1
            ),
            ExtractionPipeline(
                3, external_id="pipelineC", name="Pipeline C", data_set_id=123, created_time=1, last_updated_time=1
            ),
        ]
    )


class TestExtractionPipelineFinder:
    def test_select_extraction_pipelines(
        self, three_extraction_pipelines: ExtractionPipelineList, monkeypatch: MonkeyPatch
    ) -> None:
        def select_pipelines(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_extraction_pipelines)
            return [choices[1].value, choices[2].value]

        answers = [select_pipelines]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(ExtractionPipelineFinder.__module__, monkeypatch, answers),
        ):
            client.extraction_pipelines.list.return_value = three_extraction_pipelines
            finder = ExtractionPipelineFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("pipelineB", "pipelineC")


class TestDumpExtractionPipeline:
    def test_dump_extraction_pipelines(
        self, three_extraction_pipelines: ExtractionPipelineList, tmp_path: Path
    ) -> None:
        with monkeypatch_toolkit_client() as toolkit_client:
            approval_client = ApprovalToolkitClient(toolkit_client, allow_reverse_lookup=True)
            approval_client.append(ExtractionPipeline, three_extraction_pipelines[1:])
            toolkit_client.extraction_pipelines.config.retrieve.side_effect = CogniteAPIError(
                "There is no config stored for pipeline", code=404
            )

            client = approval_client.mock_client
            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                ExtractionPipelineFinder(client, ("pipelineB", "pipelineC")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = ExtractionPipelineCRUD(client, None, None)

            filepaths = list(loader.find_files(tmp_path))
            items = sorted(
                [read_yaml_file(filepath) for filepath in filepaths],
                key=lambda d: d.get("external_id", d.get("externalId")),
            )
            expected = sorted(
                [loader.dump_resource(ep) for ep in three_extraction_pipelines[1:]],
                key=lambda d: d.get("external_id", d.get("externalId")),
            )
            assert items == expected


@pytest.fixture()
def three_groups() -> GroupList:
    return GroupList(
        [
            Group(
                "Group A",
                source_id="123",
                capabilities=[TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All())],
            ),
            Group(
                "Group B",
                source_id="456",
                capabilities=[TimeSeriesAcl([TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.All())],
            ),
            Group(
                "Group C",
                source_id="789",
                capabilities=[
                    TimeSeriesAcl([TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.All())
                ],
            ),
        ]
    )


class TestGroupFinder:
    def test_select_groups(self, three_groups: GroupList, monkeypatch: MonkeyPatch) -> None:
        def select_groups(choices: list[Choice]) -> list[list[Group]]:
            assert len(choices) == len(three_groups)
            return [choices[1].value, choices[2].value]

        answers = [select_groups]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(GroupFinder.__module__, monkeypatch, answers),
        ):
            client.iam.groups.list.return_value = three_groups
            finder = GroupFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("Group B", "Group C")


class TestDumpGroups:
    def test_dump_groups(self, three_groups: GroupList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.iam.groups.list.return_value = three_groups

            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                GroupFinder(client, ("Group B", "Group C")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = GroupAllScopedCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = sorted([read_yaml_file(filepath) for filepath in filepaths], key=lambda d: d.get("name"))
        expected = sorted([loader.dump_resource(group) for group in three_groups[1:]], key=lambda d: d.get("name"))
        assert items == expected


@pytest.fixture()
def three_functions() -> FunctionList:
    return FunctionList(
        [
            Function(
                external_id="functionA",
                name="Function A",
                description="This is Function A",
                created_time=1,
                file_id=123,
            ),
            Function(
                external_id="functionB",
                name="Function B",
                description="This is Function B",
                created_time=1,
                file_id=456,
            ),
            Function(
                external_id="functionC",
                name="Function C",
                description="This is Function C",
                created_time=1,
                file_id=789,
            ),
        ]
    )


class TestDumpFunctions:
    def test_dump_functions(self, three_functions: FunctionList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.functions.retrieve_multiple.return_value = three_functions[1:]
            client.functions.status.return_value = FunctionsStatus("activated")
            client.files.retrieve.return_value = None
            client.files.download_bytes.side_effect = CogniteAPIError(
                "File ids not found",
                code=400,
            )

            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                FunctionFinder(client, ("functionB", "functionC")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = FunctionCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = sorted([read_yaml_file(filepath) for filepath in filepaths], key=lambda d: d["externalId"])
        expected = sorted([loader.dump_resource(func) for func in three_functions[1:]], key=lambda d: d["externalId"])
        assert items == expected

    def test_interactive_select_functions(self, three_functions: FunctionList, monkeypatch: MonkeyPatch) -> None:
        def select_functions(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_functions)
            return [choices[1].value, choices[2].value]

        answers = [select_functions]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(FunctionFinder.__module__, monkeypatch, answers),
        ):
            client.functions.list.return_value = three_functions
            finder = FunctionFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("functionB", "functionC")


@pytest.fixture()
def three_datasets() -> DataSetList:
    return DataSetList(
        [
            DataSet(external_id="datasetA", name="Dataset A", created_time=1, last_updated_time=2),
            DataSet(external_id="datasetB", name="Dataset B", created_time=1, last_updated_time=2),
            DataSet(external_id="datasetC", name="Dataset C", created_time=1, last_updated_time=2),
        ]
    )


class TestDataSetFinder:
    def test_select_datasets(self, three_datasets: DataSetList, monkeypatch: MonkeyPatch) -> None:
        def select_datasets(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_datasets)
            return [choices[1].value, choices[2].value]

        answers = [select_datasets]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DataSetFinder.__module__, monkeypatch, answers),
        ):
            client.data_sets.list.return_value = three_datasets
            finder = DataSetFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("datasetB", "datasetC")


class TestDumpDataSets:
    def test_dump_datasets(self, three_datasets: DataSetList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_sets.retrieve_multiple.return_value = three_datasets[1:]

            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                DataSetFinder(client, ("datasetB", "datasetC")),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = DataSetsCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 2
        items = sorted([read_yaml_file(filepath) for filepath in filepaths], key=lambda d: d["externalId"])
        expected = sorted([loader.dump_resource(ds) for ds in three_datasets[1:]], key=lambda d: d["externalId"])
        assert items == expected


@pytest.fixture()
def three_streamlit_apps() -> StreamlitList:
    return StreamlitList(
        [
            Streamlit(
                external_id="appA",
                name="App A",
                description="This is App A",
                created_time=1,
                last_updated_time=1,
                entrypoint="main.py",
                creator="me",
            ),
            Streamlit(
                external_id="appB",
                name="App B",
                description="This is App B",
                created_time=1,
                last_updated_time=1,
                entrypoint="main.py",
                creator="me",
            ),
            Streamlit(
                external_id="appC",
                name="App C",
                description="This is App C",
                created_time=1,
                last_updated_time=1,
                entrypoint="main.py",
                creator="me",
            ),
        ]
    )


class TestDumpStreamlitApps:
    def test_interactive_select_streamlit_apps(
        self, three_streamlit_apps: StreamlitList, monkeypatch: MonkeyPatch
    ) -> None:
        def select_streamlit_apps(choices: list[Choice]) -> list[str]:
            assert len(choices) == len(three_streamlit_apps)
            return [choices[1].value, choices[2].value]

        answers = ["me", select_streamlit_apps]

        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(StreamlitFinder.__module__, monkeypatch, answers),
        ):
            client.files.list.return_value = FileMetadataList([app.as_file() for app in three_streamlit_apps])
            counts_by_creator = Counter([app.creator for app in three_streamlit_apps])
            client.documents.aggregate_unique_values.return_value = UniqueResultList(
                [UniqueResult(count=count, values=[creator]) for creator, count in counts_by_creator.items()]
            )
            finder = StreamlitFinder(client, None)
            selected = finder._interactive_select()

        assert selected == ("appB", "appC")

    def test_dump_streamlit_app(self, three_streamlit_apps: StreamlitList, tmp_path: Path) -> None:
        with monkeypatch_toolkit_client() as client:
            client.files.retrieve_multiple.return_value = FileMetadataList([three_streamlit_apps[1].as_file()])
            client.files.download_bytes.return_value = json.dumps(
                {
                    "entrypoint": "main.py",
                    "files": {
                        "main.py": {
                            "content": {
                                "text": 'import streamlit as st\nfrom cognite.client import CogniteClient\n\nst.title("An example app in CDF")\nclient = CogniteClient()\n\n\n@st.cache_data\ndef get_assets():\n    assets = client.assets.list(limit=1000).to_pandas()\n    assets = assets.fillna(0)\n    return assets\n\n\nst.write(get_assets())\n',
                                "$case": "text",
                            }
                        }
                    },
                    "requirements": ["pyodide-http==0.2.1", "cognite-sdk==7.51.1"],
                }
            ).encode("utf-8")

            cmd = DumpResourceCommand(silent=True)
            cmd.dump_to_yamls(
                StreamlitFinder(client, ("appB",)),
                output_dir=tmp_path,
                clean=False,
                verbose=False,
            )
            loader = StreamlitCRUD(client, None, None)

        filepaths = list(loader.find_files(tmp_path))
        assert len(filepaths) == 1
        config_file = filepaths[0]
        loaded = read_yaml_file(config_file)
        expected = loader.dump_resource(three_streamlit_apps[1])
        assert loaded == expected

        app_dir = config_file.parent / "appB"
        assert app_dir.is_dir()
        app_files = list(app_dir.iterdir())
        assert set(app_files) == {
            app_dir / "main.py",
            app_dir / "requirements.txt",
        }

    @pytest.mark.parametrize(
        "content,expected_warning,expected_severity",
        [
            pytest.param(
                None,
                "The source code for 'appB' could not be retrieved from CDF.",
                "high",
                id="File not found",
            ),
            pytest.param(
                '{"not": "json"',
                "The JSON content for the Streamlit app 'appB' is corrupt and could not be "
                "extracted. Download file with the same external id manually to remediate.",
                "high",
                id="Corrupted JSON",
            ),
            pytest.param(
                json.dumps({"requirements": [], "files": {}, "entrypoint": "main.py"}),
                "The Streamlit app 'appB' does not have any files to dump. It is likely corrupted.",
                "high",
                id="No files in JSON",
            ),
            pytest.param(
                json.dumps(
                    {
                        "requirements": ["foo==1.0"],
                        "files": {
                            "page/CDF_demo.py": {"content": {"text": True}},
                            "main.py": {"content": {"text": "print('hi')"}},
                        },
                        "entrypoint": "main.py",
                    }
                ),
                "The Streamlit app 'appB' has a file page/CDF_demo.py with invalid content. Skipping...",
                "high",
                id="File with no content",
            ),
            pytest.param(
                json.dumps(
                    {
                        "requirements": ["foo==1.0"],
                        "files": {"main.py": {"content": {"text": "print('hi')"}}},
                        "entrypoint": "notfound.py",
                    }
                ),
                "The Streamlit app 'appB' has an entry point notfound.py that was not found in the files. The app may be corrupted.",
                "high",
                id="Entrypoint not found",
            ),
        ],
    )
    def test_dump_code(
        self,
        content: str | None,
        expected_warning: str,
        expected_severity: str,
        three_streamlit_apps: StreamlitList,
        tmp_path: Path,
    ) -> None:
        console = MagicMock(spec=Console)
        app = three_streamlit_apps[1]
        with monkeypatch_toolkit_client() as client:
            if content is None:
                client.files.download_bytes.side_effect = CogniteAPIError(
                    message=f"Files ids not found: {app.external_id}", code=400, missing=[app.external_id]
                )
            else:
                client.files.download_bytes.return_value = content.encode("utf-8")

            finder = StreamlitFinder(client, (app.external_id,))
            finder.dump_code(app, tmp_path, console)

            assert console.print.call_count == 1
            severity, actual_message = console.print.call_args.args
            assert expected_warning in actual_message
            assert expected_severity in str(severity).casefold()
