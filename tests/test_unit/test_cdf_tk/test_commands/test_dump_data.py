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

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DumpDataCommand
from cognite_toolkit._cdf_tk.commands.dump_data import AssetFinder, TimeSeriesFinder
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file


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
        cmd = DumpDataCommand(skip_tracking=False, print_warning=False)
        output_dir = tmp_path / "asset_dump"
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = [AssetList([my_asset])]
            client.assets.aggregate_count.return_value = 1
            client.assets.retrieve.return_value = Asset(external_id="rootAsset")
            client.labels.retrieve.return_value = LabelDefinitionList([my_label])
            client.data_sets.retrieve_multiple.return_value = [dataset]
            client.lookup.assets.external_id.return_value = "rootAsset"
            client.lookup.data_sets.external_id.return_value = dataset.external_id

            cmd.dump_table(
                AssetFinder(client, ["rootAsset"], []),
                output_dir,
                clean=True,
                limit=None,
                format_="csv",
                verbose=False,
            )

        output_csv = next(output_dir.rglob("*.csv"))
        assert output_csv.read_text().splitlines() == [
            "dataSetExternalId,description,externalId,labels,metadata.key,name,source",
            "my_dataset,This is my asset,my_asset,['label1'],value,My Asset,MySource",
        ]

        dataset_yaml = next(output_dir.rglob("*DataSet.yaml"))
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]

        label_yaml = next(output_dir.rglob("*Label.yaml"))
        assert read_yaml_file(label_yaml) == [my_label.as_write().dump()]

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
        cmd = DumpDataCommand(skip_tracking=False, print_warning=False)
        output_dir = tmp_path / "timeseries_dump"
        with monkeypatch_toolkit_client() as client:
            client.time_series.return_value = [TimeSeriesList([my_timeseries])]
            client.time_series.aggregate_count.return_value = 1
            client.data_sets.retrieve_multiple.return_value = [dataset]
            client.lookup.data_sets.external_id.return_value = dataset.external_id

            cmd.dump_table(
                TimeSeriesFinder(client, [], [dataset.external_id]),
                output_dir,
                clean=True,
                limit=None,
                format_="csv",
                verbose=False,
            )

        output_csv = next(output_dir.rglob("*.csv"))
        assert output_csv.read_text().splitlines() == [
            "dataSetExternalId,description,externalId,isStep,isString,metadata.key,name",
            "my_dataset,This is my timeseries,my_timeseries,False,False,value,My TimeSeries",
        ]

        dataset_yaml = next(output_dir.rglob("*DataSet.yaml"))
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]
