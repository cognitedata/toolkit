from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply

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
                            resource_type="timeseries",
                            id=123,
                            data_set_id=123,
                            instance_id=NodeId("sp_full_ts", "full_ts_id"),
                        ),
                        MigrationMapping(
                            resource_type="timeseries",
                            id=3231,
                            data_set_id=None,
                            instance_id=NodeId("sp_step_ts", "step_ts_id"),
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
                            resource_type="timeseries",
                            id=230,
                            data_set_id=None,
                            instance_id=NodeId("my_space", "target_external_id"),
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
                            resource_type="timeseries",
                            id=42,
                            data_set_id=123,
                            instance_id=NodeId("sp_full_ts", "full_ts_id"),
                        )
                    ]
                ),
                id="Mapping with BOM",
            ),
        ],
    )
    def test_read_mapping_file(self, content: str, expected: MigrationMappingList, tmp_path: Path) -> None:
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")
        actual = MigrationMappingList.read_mapping_file(input_file, resource_type="timeseries")
        assert actual == expected

    @pytest.mark.parametrize(
        "content, expected_msg",
        [
            pytest.param("", "Mapping file is empty", id="empty_file"),
            pytest.param(
                "space,externalId,id,dataSetId\n",
                (
                    "Invalid mapping file header:\n"
                    " - First column must be 'id'. Got 'space'.\n"
                    " - If there are 4 columns, the second column must be 'dataSetId'. Got 'externalId'.\n"
                    " - Last two columns must be 'space' and 'externalId'. Got 'id' and 'dataSetId'."
                ),
                id="invalid header",
            ),
            pytest.param(
                "id,data_set_id,space,externalId\n",
                (
                    "Invalid mapping file header:\n - If there are 4 columns, "
                    "the second column must be 'dataSetId'. Got 'data_set_id'."
                ),
                id="invalid header with data_set_id",
            ),
            pytest.param(
                "id,externalId\n",
                "Invalid mapping file header:\n"
                " - Mapping file must have at least 3 columns: id, space, "
                "externalId. Got 2 columns.\n"
                " - Last two columns must be 'space' and 'externalId'. Got 'id' and 'externalId'.",
                id="Too few columns",
            ),
            pytest.param(
                "externalId,dataSetId,space,externalId,myExtra\n",
                "Invalid mapping file header:\n"
                " - Mapping file must have at most 4 columns: id, dataSetId, "
                "space, externalId. Got 5 columns.\n"
                " - First column must be 'id'. Got 'externalId'.\n"
                " - Last two columns must be 'space' and 'externalId'. Got 'externalId' and 'myExtra'.",
                id="Too many columns",
            ),
            pytest.param(
                "id,dataSetId,space,externalId\n123,123,sp_full_ts,full_ts_id\ninvalid_id,,sp_step_ts,step_ts_id\n",
                "Invalid ID or dataSetId in row 2: ['invalid_id', '', 'sp_step_ts', "
                "'step_ts_id']. ID and dataSetId must be integers.",
                id="Invalid id value",
            ),
            pytest.param(
                "id,dataSetId,space,externalId\n123,invalid_dataset_id,sp_full_ts,full_ts_id\n",
                "Invalid ID or dataSetId in row 1: ['123', 'invalid_dataset_id', "
                "'sp_full_ts', 'full_ts_id']. ID and dataSetId must be integers.",
                id="Invalid external_id value",
            ),
            pytest.param(
                "\n",
                (
                    "Invalid mapping file header:\n"
                    " - Mapping file must have at least 3 columns: id, space, "
                    "externalId. Got 0 columns."
                ),
                id="Empty header row",
            ),
        ],
    )
    def test_read_invalid_mapping_file(self, content: str, expected_msg: str, tmp_path: Path) -> None:
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")

        with pytest.raises(ValueError) as exc_info:
            MigrationMappingList.read_mapping_file(input_file, resource_type="timeseries")
        assert str(exc_info.value) == expected_msg
