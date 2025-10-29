from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import NodeId, ViewId

from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    MigrationMappingList,
    TimeSeriesMapping,
    TimeSeriesMigrationMappingList,
)


class TestMigrationMappingList:
    @pytest.mark.parametrize(
        "content, expected",
        [
            pytest.param(
                "id,dataSetId,space,externalId\n123,123,sp_full_ts,full_ts_id\n3231,,sp_step_ts,step_ts_id\n",
                MigrationMappingList(
                    [
                        TimeSeriesMapping(
                            id=123,
                            dataSetId=123,
                            instanceId=NodeId("sp_full_ts", "full_ts_id"),
                        ),
                        TimeSeriesMapping(
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
                        TimeSeriesMapping(
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
                        TimeSeriesMapping(
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
                        TimeSeriesMapping(
                            id=123,
                            dataSetId=123,
                            instanceId=NodeId("sp_full_ts", "full_ts_id"),
                            ingestionView="ingestion_view_id",
                            preferredConsumerView=ViewId(
                                space="consumer_view_space", external_id="consumer_view_external_id", version="1.0"
                            ),
                        ),
                        TimeSeriesMapping(
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
        actual = TimeSeriesMigrationMappingList.read_csv_file(input_file)
        assert not actual.invalid_rows
        assert actual == expected

    @pytest.mark.parametrize(
        "content, expected_msg",
        [
            pytest.param("", "No data found in the file: '{filepath}'.", id="empty_file"),
            pytest.param(
                "id,externalId\n123,full_ts_id\n",
                "Missing required columns: space",
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
            TimeSeriesMigrationMappingList.read_csv_file(input_file)
        assert str(exc_info.value) == expected_msg

    @pytest.mark.parametrize(
        "content, unexpected_columns",
        [
            pytest.param(
                "id,dataSetId,space,externalId,unexpected_col\n123,123,sp_full_ts,full_ts_id,some_value\n",
                {"unexpected_col"},
                id="Unexpected column",
            ),
        ],
    )
    def test_read_with_warnings(self, content: str, unexpected_columns: list[str], tmp_path: Path) -> None:
        input_file = tmp_path / "mapping_file.csv"
        input_file.write_text(content, encoding="utf-8")

        mapping = TimeSeriesMigrationMappingList.read_csv_file(input_file)
        assert mapping.unexpected_columns == unexpected_columns
