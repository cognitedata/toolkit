from pathlib import Path

from cognite.client.data_classes import (
    Asset,
    AssetList,
    DataSet,
    GeoLocation,
    Geometry,
    LabelDefinition,
    LabelDefinitionList,
    TimeSeries,
    TimeSeriesList,
    TransformationPreviewResult,
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
            geo_location=GeoLocation(
                type="Feature",
                geometry=Geometry(type="LineString", coordinates=[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]]),
                properties={},
            ),
        )
        cmd = DumpDataCommand(skip_tracking=False, print_warning=False)
        output_dir = tmp_path / "asset_dump"
        csv_dir = output_dir / "csv"
        parquet_dir = output_dir / "parquet"
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = [AssetList([my_asset])]
            client.assets.aggregate_count.return_value = 1
            client.assets.retrieve.return_value = Asset(external_id="rootAsset")
            client.labels.retrieve.return_value = LabelDefinitionList([my_label])
            client.data_sets.retrieve_multiple.return_value = [dataset]
            client.lookup.assets.external_id.return_value = "rootAsset"
            client.lookup.data_sets.external_id.return_value = dataset.external_id
            client.transformations.preview.return_value = TransformationPreviewResult(
                None, [{"key": "key", "key_count": 1}]
            )

            cmd.dump_table(
                AssetFinder(client, ["rootAsset"], []),
                csv_dir,
                clean=True,
                limit=None,
                format_="csv",
                verbose=False,
            )
            cmd.dump_table(
                AssetFinder(client, [], [dataset.external_id]),
                parquet_dir,
                clean=True,
                limit=None,
                format_="parquet",
                verbose=False,
            )

        output_csvs = list(csv_dir.rglob("*.csv"))
        assert len(output_csvs) == 1
        output_csv = output_csvs[0]
        assert output_csv.read_text().splitlines() == [
            "externalId,name,parentExternalId,description,dataSetExternalId,source,labels,geoLocation,metadata.key",
            "my_asset,My Asset,,This is my asset,my_dataset,MySource,['label1'],"
            "\"{'type': 'Feature', 'geometry': {'type': 'LineString', 'coordinates': [[1.0, 1.0], [2.0, 2.0], "
            "[3.0, 3.0]]}, 'properties': {}}\""
            ",value",
        ]

        dataset_yamls = list(csv_dir.rglob("*DataSet.yaml"))
        assert len(dataset_yamls) == 1
        dataset_yaml = dataset_yamls[0]
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]

        label_yamls = list(csv_dir.rglob("*Label.yaml"))
        assert len(label_yamls) == 1
        label_yaml = label_yamls[0]
        assert read_yaml_file(label_yaml) == [my_label.as_write().dump()]

        parquet_files = list(parquet_dir.rglob("*.parquet"))
        assert len(parquet_files) == 1

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
            client.transformations.preview.return_value = TransformationPreviewResult(
                None, [{"key": "key", "key_count": 1}]
            )

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
            "externalId,name,isString,unit,unitExternalId,assetExternalId,isStep,description,dataSetExternalId,securityCategories,metadata.key",
            "my_timeseries,My TimeSeries,False,,,,False,This is my timeseries,my_dataset,,value",
        ]

        dataset_yaml = next(output_dir.rglob("*DataSet.yaml"))
        assert read_yaml_file(dataset_yaml) == [dataset.as_write().dump()]
