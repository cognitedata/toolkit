from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Transformation, TransformationList, TransformationScheduleList
from questionary import Choice

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder, DumpResourceCommand, TransformationFinder
from cognite_toolkit._cdf_tk.loaders import TransformationLoader
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
            loader = TransformationLoader(client, None, None)

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
