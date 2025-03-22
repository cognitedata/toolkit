from pathlib import Path

from cognite.client.data_classes import (
    Asset,
    AssetList,
    DataSet,
    LabelDefinition,
    LabelDefinitionList,
    TimeSeries,
    TimeSeriesList,
)
from questionary import Choice

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DumpAssetsCommand, DumpTimeSeriesCommand
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from tests.test_unit.utils import MockQuestionary


class TestDumpData:
    def test_dump_assets(self, tmp_path: Path) -> None:
        my_label = LabelDefinition(external_id="label1", name="Label 1")
        dataset = DataSet(external_id="my_dataset", name="My Dataset", id=123)
        my_asset = Asset(
            external_id="my_asset",
            name="My Asset",
            parent_external_id=None,
            description="This is my asset",
            data_set_id=dataset.id,
            metadata={"key": "value"},
            source="MySource",
            labels=[my_label.external_id],
        )
        cmd = DumpAssetsCommand(skip_tracking=False, print_warning=False)
        output_dir = tmp_path / "asset_dump"
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = [AssetList([my_asset])]
            client.assets.aggregate_count.return_value = 1
            client.assets.retrieve.return_value = Asset(external_id="rootAsset")
            client.labels.retrieve.return_value = LabelDefinitionList([my_label])
            client.data_sets.retrieve.return_value = dataset

            cmd.execute(client, ["my_asset"], None, output_dir, clean=True, limit=None, format_="csv", verbose=False)

        output_csv = next(output_dir.rglob("*.csv"))
        assert output_csv.read_text().splitlines() == [
            "dataSetExternalId,description,externalId,labels,metadata.key,name,source",
            "my_dataset,This is my asset,my_asset,['label1'],value,My Asset,MySource",
        ]

        dataset_yaml = next(output_dir.rglob("*DataSet.yaml"))
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]

        label_yaml = next(output_dir.rglob("*Label.yaml"))
        assert read_yaml_file(label_yaml) == [my_label.as_write().dump()]

    def test_interactive_select_assets(self, monkeypatch) -> None:
        cmd = DumpAssetsCommand(skip_tracking=False, print_warning=False)

        def select_hierarchy(choices: list[Choice]) -> list[str]:
            assert len(choices) == 2
            return [choices[1].value]

        def select_data_set(choices: list[Choice]) -> list[str]:
            assert len(choices) == 3
            return [choices[2].value]

        answers = ["Hierarchy", select_hierarchy, "Data Set", select_data_set, "Done"]
        with (
            monkeypatch_toolkit_client() as client,
            MockQuestionary(DumpAssetsCommand.__module__, monkeypatch, answers),
        ):
            client.assets.return_value = [Asset(id=1, external_id="Root1"), Asset(id=2, external_id="Root2")]
            client.assets.aggregate_count.return_value = 100
            client.data_sets = [
                DataSet(id=1, external_id="dataset1"),
                DataSet(id=2, external_id="dataset2"),
                DataSet(id=3, external_id="dataset3"),
            ]

            selected_hierarchy, selected_dataset = cmd._select_hierarchy_and_data_set(
                client, None, None, interactive=True
            )

        assert selected_hierarchy == ["Root2"]
        assert selected_dataset == ["dataset3"]

    def test_dump_timeseries(self, tmp_path: Path) -> None:
        dataset = DataSet(external_id="my_dataset", name="My Dataset", id=123)
        my_timeseries = TimeSeries(
            external_id="my_timeseries",
            name="My TimeSeries",
            description="This is my timeseries",
            metadata={"key": "value"},
            is_string=False,
            is_step=False,
            data_set_id=dataset.id,
        )
        cmd = DumpTimeSeriesCommand(skip_tracking=False, print_warning=False)
        output_dir = tmp_path / "timeseries_dump"
        with monkeypatch_toolkit_client() as client:
            client.time_series.return_value = [TimeSeriesList([my_timeseries])]
            client.time_series.aggregate_count.return_value = 1
            client.data_sets.retrieve.return_value = dataset

            cmd.execute(
                client, [dataset.external_id], None, output_dir, clean=True, limit=None, format_="csv", verbose=False
            )

        output_csv = next(output_dir.rglob("*.csv"))
        assert output_csv.read_text().splitlines() == [
            "dataSetExternalId,description,externalId,isStep,isString,metadata.key,name",
            "my_dataset,This is my timeseries,my_timeseries,False,False,value,My TimeSeries",
        ]

        dataset_yaml = next(output_dir.rglob("*DataSet.yaml"))
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]
