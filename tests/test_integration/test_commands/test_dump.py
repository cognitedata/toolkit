import random
from pathlib import Path

import numpy as np
import pytest
from cognite.client.data_classes import (
    AssetList,
    AssetWrite,
    AssetWriteList,
    DataSet,
    DataSetWrite,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
)
from pytest_regressions.file_regression import FileRegressionFixture

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DumpAssetsCommand, DumpTimeSeriesCommand
from tests.utils import rng_context


@pytest.fixture(scope="session")
def dump_data_set(toolkit_client: ToolkitClient) -> DataSet:
    dump_ds = DataSetWrite(
        external_id="ds_dump",
        name="Dump dataset",
        description="This dataset is used for testing dump command",
    )
    if existing := toolkit_client.data_sets.retrieve(external_id=dump_ds.external_id):
        return existing

    return toolkit_client.data_sets.create(dump_ds)


@rng_context(seed=42)
def generate_asset_tree(
    root: AssetWrite, first_level_size: int, size: int, depth: int, data_set_id: int | None = None
) -> AssetWriteList:
    # A power law distribution describes the real shape of an asset hierarchy, i.e., few roots, many leaves.
    count_per_level = np.random.power(0.2, depth)
    count_per_level.sort()
    total = count_per_level.sum()
    count_per_level = (count_per_level / total) * size
    count_per_level = np.ceil(count_per_level).astype(np.int64)
    count_per_level[0] = first_level_size
    count_per_level[-1] = size - count_per_level[:-1].sum() - 1
    last_level = [root]
    hierarchy = AssetWriteList([root])
    for level, count in enumerate(count_per_level, 1):
        this_level = []
        for asset_no in range(count):
            parent = random.choice(last_level)
            identifier = f"test__asset_depth_{level}_asset_{asset_no}"
            asset = AssetWrite(
                name=f"Asset {asset_no} depth@{level}",
                external_id=identifier,
                parent_external_id=parent.external_id,
                data_set_id=data_set_id,
            )
            this_level.append(asset)
        last_level = this_level
        hierarchy.extend(this_level)
    return hierarchy


@rng_context(seed=42)
def generate_timeseries(
    count: int, asset_ids: list[int] | None = None, data_set_id: int | None = None
) -> TimeSeriesWriteList:
    return TimeSeriesWriteList(
        [
            TimeSeriesWrite(
                external_id=f"test__timeseries_{i}",
                name=f"Timeseries {i}",
                is_step=random.choice([True, False]),
                is_string=random.choice([True, False]),
                asset_id=random.choice(asset_ids) if asset_ids else None,
                data_set_id=data_set_id,
            )
            for i in range(count)
        ]
    )


@pytest.fixture(scope="session")
def asset_hierarchy(toolkit_client: ToolkitClient, dump_data_set: DataSet) -> AssetList:
    root = AssetWrite(name="Root", external_id="test__asset_root", data_set_id=dump_data_set.id)
    hierarchy = generate_asset_tree(root, first_level_size=5, size=100, depth=5, data_set_id=dump_data_set.id)
    existing = toolkit_client.assets.retrieve_multiple(
        external_ids=hierarchy.as_external_ids(), ignore_unknown_ids=True
    )
    if len(existing) != len(hierarchy):
        existing = toolkit_client.assets.create_hierarchy(hierarchy, upsert=True, upsert_mode="patch")

    return existing


@pytest.fixture(scope="session")
def some_timeseries(toolkit_client: ToolkitClient, dump_data_set: DataSet, asset_hierarchy: AssetList) -> None:
    asset_ids = [asset.id for asset in asset_hierarchy]
    timeseries = generate_timeseries(100, asset_ids, dump_data_set.id)
    existing = toolkit_client.time_series.retrieve_multiple(
        external_ids=timeseries.as_external_ids(), ignore_unknown_ids=True
    )
    if len(existing) != len(timeseries):
        existing = toolkit_client.time_series.upsert(timeseries, mode="patch")
    return existing


class TestDumpDataCommand:
    def test_dump_asset(
        self,
        toolkit_client: ToolkitClient,
        asset_hierarchy: AssetList,
        tmp_path: Path,
        file_regression: FileRegressionFixture,
    ) -> None:
        dump_command = DumpAssetsCommand(skip_tracking=True, print_warning=False)
        first = asset_hierarchy[0]
        root = next(asset for asset in asset_hierarchy if first.root_id == asset.id)
        assert first.external_id is not None
        dump_command.execute(toolkit_client, [root.external_id], None, tmp_path / "asset", False, None, "csv", False)

        dumped_files = list((tmp_path / "asset").rglob("*.csv"))
        assert len(dumped_files) == 1
        file_regression.check(dumped_files[0].read_text(), encoding="utf-8")

    def test_dump_timeseries(
        self,
        toolkit_client: ToolkitClient,
        some_timeseries: TimeSeriesList,
        dump_data_set: DataSet,
        tmp_path: Path,
        file_regression: FileRegressionFixture,
    ) -> None:
        dump_command = DumpTimeSeriesCommand(skip_tracking=True, print_warning=False)
        dump_command.execute(
            toolkit_client, [dump_data_set.external_id], None, tmp_path / "timeseries", False, None, "csv", False
        )

        dumped_files = list((tmp_path / "timeseries").rglob("*.csv"))
        assert len(dumped_files) == 1
        file_regression.check(dumped_files[0].read_text(), encoding="utf-8")
