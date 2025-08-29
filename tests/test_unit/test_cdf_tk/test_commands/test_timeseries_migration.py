from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeId, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries
from cognite_toolkit._cdf_tk.commands import MigrateTimeseriesCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    MigrationMapping,
    MigrationMappingList,
)


class TestMigrateTimeSeriesCommand:
    @pytest.mark.parametrize(
        "ts, expected",
        [
            (
                ExtendedTimeSeries(
                    external_id="full_ts",
                    data_set_id=123,
                    description="Test description",
                    name="Test Time Series",
                    is_step=True,
                    is_string=False,
                    unit="m/s",
                    unit_external_id="velocity:m-per-sec",
                    pending_instance_id=NodeId("sp_full_ts", "full_ts_id"),
                ),
                CogniteTimeSeriesApply(
                    space="sp_full_ts",
                    external_id="full_ts_id",
                    is_step=True,
                    time_series_type="numeric",
                    name="Test Time Series",
                    description="Test description",
                    source_unit="m/s",
                    unit=DirectRelationReference("cdf_cdm_units", "velocity:m-per-sec"),
                ),
            ),
            (
                ExtendedTimeSeries(
                    external_id="minimum_ts",
                    is_step=False,
                    is_string=True,
                    pending_instance_id=NodeId("sp_step_ts", "step_ts_id"),
                ),
                CogniteTimeSeriesApply(
                    space="sp_step_ts",
                    external_id="step_ts_id",
                    is_step=False,
                    time_series_type="string",
                ),
            ),
        ],
    )
    def test_as_cognite_timeseries(self, ts: ExtendedTimeSeries, expected: CogniteTimeSeriesApply) -> None:
        actual = MigrateTimeseriesCommand.as_cognite_timeseries(ts)

        assert actual.dump() == expected.dump()


class TestMigrationMappingList:
    @pytest.mark.parametrize(
        "content, expected",
        [
            pytest.param(
                "id,dataSetId,space,externalId\n123,123,sp_full_ts,full_ts_id\n3231,,sp_step_ts,step_ts_id\n",
                MigrationMappingList(
                    [
                        MigrationMapping(
                            resourceType="timeseries",
                            id=123,
                            dataSetId=123,
                            instanceId=NodeId("sp_full_ts", "full_ts_id"),
                        ),
                        MigrationMapping(
                            resourceType="timeseries",
                            id=3231,
                            dataSetId=None,
                            instanceId=NodeId("sp_step_ts", "step_ts_id"),
                        ),
                    ]
                ),
                id="Mapping IDs with dataSetId",
            ),
            pytest.param(
                "id,space,externalId\n230,my_space,target_external_id\n",
                MigrationMappingList(
                    [
                        MigrationMapping(
                            resourceType="timeseries",
                            id=230,
                            dataSetId=None,
                            instanceId=NodeId("my_space", "target_external_id"),
                        )
                    ]
                ),
                id="Mapping IDs without dataSetId",
            ),
            pytest.param(
                """\ufeffid,dataSetId,space,externalId\n42,123,sp_full_ts,full_ts_id\n""",
                MigrationMappingList(
                    [
                        MigrationMapping(
                            resourceType="timeseries",
                            id=42,
                            dataSetId=123,
                            instanceId=NodeId("sp_full_ts", "full_ts_id"),
                        )
                    ]
                ),
                id="Mapping with BOM",
            ),
            pytest.param(
                """id,space,externalId,dataSetId,ingestionView,consumerViewSpace,consumerViewExternalId,consumerViewVersion\n
123,sp_full_ts,full_ts_id,123,ingestion_view_id,consumer_view_space,consumer_view_external_id,1.0\n
3231,sp_step_ts,step_ts_id,,ingestion_view_id_2,consumer_view_space_2,consumer_view_external_id_2,2.0\n""",
                MigrationMappingList(
                    [
                        MigrationMapping(
                            resourceType="timeseries",
                            id=123,
                            dataSetId=123,
                            instanceId=NodeId("sp_full_ts", "full_ts_id"),
                            ingestionView="ingestion_view_id",
                            preferredConsumerView=ViewId(
                                space="consumer_view_space", external_id="consumer_view_external_id", version="1.0"
                            ),
                        ),
                        MigrationMapping(
                            resourceType="timeseries",
                            id=3231,
                            dataSetId=None,
                            instanceId=NodeId("sp_step_ts", "step_ts_id"),
                            ingestionView="ingestion_view_id_2",
                            preferredConsumerView=ViewId(
                                space="consumer_view_space_2", external_id="consumer_view_external_id_2", version="2.0"
                            ),
                        ),
                    ]
                ),
                id="Mapping with all columns including optional ones",
            ),
        ],
    )
    def test_read_mapping_file(self, content: str, expected: MigrationMappingList, tmp_path: Path) -> None:
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")
        actual = MigrationMappingList.read_mapping_file(input_file, resource_type="timeseries")
        assert not actual.error_by_row_no
        assert actual == expected

    @pytest.mark.parametrize(
        "content, expected_msg",
        [
            pytest.param("", "No data found in the file: '{filepath}'.", id="empty_file"),
            pytest.param(
                "id,externalId\n123,full_ts_id\n",
                "Missing required columns in mapping file: space.",
                id="Too few columns",
            ),
        ],
    )
    def test_read_invalid_mapping_file(self, content: str, expected_msg: str, tmp_path: Path) -> None:
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")
        if "{filepath}" in expected_msg:
            expected_msg = expected_msg.format(filepath=input_file.as_posix())

        with pytest.raises(ValueError) as exc_info:
            MigrationMappingList.read_mapping_file(input_file, resource_type="timeseries")
        assert str(exc_info.value) == expected_msg

    @pytest.mark.parametrize(
        "content, expected_warnings",
        [
            pytest.param(
                "id,dataSetId,space,externalId,unexpected_col\n123,123,sp_full_ts,full_ts_id,some_value\n",
                ["Ignoring unexpected columns in mapping file: unexpected_col"],
                id="Unexpected column",
            ),
        ],
    )
    def test_read_with_warnings(self, content: str, expected_warnings: list[str], tmp_path: Path) -> None:
        actual_warnings: list[str] = []
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")
        fake_console = MagicMock(spec=Console)

        def fake_print(prefix: str, message: str, *_, **__) -> None:
            actual_warnings.append(message)

        fake_console.print.side_effect = fake_print

        MigrationMappingList.read_mapping_file(input_file, resource_type="timeseries", console=fake_console)
        assert actual_warnings == expected_warnings
