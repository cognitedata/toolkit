import pytest
from cognite.client.data_classes import (
    Database,
    Table,
    Transformation,
    TransformationDestination,
    TransformationPreviewResult,
)
from rich.spinner import Spinner

from cognite_toolkit._cdf_tk.client.data_classes.raw import (
    RawProfileColumns,
    RawProfileResults,
    StringProfile,
    StringProfileColumn,
)
from cognite_toolkit._cdf_tk.commands import ProfileRawCommand
from cognite_toolkit._cdf_tk.constants import MAX_ROW_ITERATION_RUN_QUERY
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture()
def raw_profile_results_single_column() -> RawProfileResults:
    return RawProfileResults(
        row_count=ProfileRawCommand.max_profile_raw_count,
        columns=RawProfileColumns(
            {
                "externalId": StringProfileColumn(
                    count=500,
                    null_count=ProfileRawCommand.max_profile_raw_count - 500,
                    string=StringProfile(
                        length_range=(1, 10),
                        distinct_count=500,
                        length_histogram=((1, 2), (100, 200)),
                        value_counts={"test1": 100, "test2": 200, "test3": 200},
                        count=500,
                    ),
                )
            }
        ),
        is_complete=False,
    )


class TestProfileCommand:
    @pytest.mark.parametrize(
        "this_row, last_row, expected",
        [
            pytest.param(
                ["MyTable", "13", "2", "-", "-", "-"],
                ["MyTable", "12", "1", "-", "-", "-"],
                ["", "13", "2", "-", "-", "-"],
                id="No change in table name",
            ),
            pytest.param(
                ["MyTable", "13", "2", "-", "-", "-"],
                [],
                ["MyTable", "13", "2", "-", "-", "-"],
                id="Empty last row",
            ),
            pytest.param(
                ["MyTable", "13", "2", "my_transformation", "assets", "upsert"],
                ["OtherTable", "13", "2", "my_transformation", "assets", "upsert"],
                ["MyTable", "13", "2", "my_transformation", "assets", "upsert"],
                id="Change in table name with transformation",
            ),
            pytest.param(
                [
                    "MyTable",
                    Spinner(**ProfileRawCommand.spinner_args),
                    Spinner(**ProfileRawCommand.spinner_args),
                    "-",
                    "-",
                    "-",
                ],
                [
                    "MyTable",
                    Spinner(**ProfileRawCommand.spinner_args),
                    Spinner(**ProfileRawCommand.spinner_args),
                    "-",
                    "-",
                    "-",
                ],
                ["", "", "", "", "", ""],
                id="Spinner in both rows",
            ),
        ],
    )
    def test_create_draw_row(
        self, this_row: list[str | Spinner], last_row: list[str | Spinner], expected: list[str | Spinner]
    ) -> None:
        draw_row = ProfileRawCommand._create_draw_row(this_row, last_row)

        assert draw_row == expected

    def test_profile_raw_command_fallback_row_count(
        self, toolkit_client_approval: ApprovalToolkitClient, raw_profile_results_single_column: RawProfileResults
    ) -> None:
        """Test that when there is more than 10 000 rows in the table,
        the fallback is to use the /transformations/preview endpoint to get the row count."""
        cmd = ProfileRawCommand(silent=True)
        row_count = MAX_ROW_ITERATION_RUN_QUERY
        toolkit_client_approval.append(
            Transformation,
            Transformation(
                external_id="MyTransformation",
                name="My Transformation",
                conflict_mode="update",
                query="SELECT externalId, name FROM `database`.`table`",
                destination=TransformationDestination(type="events"),
            ),
        )
        toolkit_client_approval.append(Database, Database("database"))
        toolkit_client_approval.append(Table, Table("table"))

        toolkit_client_approval.mock_client.raw.profile.return_value = raw_profile_results_single_column
        toolkit_client_approval.mock_client.transformations.preview.return_value = TransformationPreviewResult(
            results=[{"row_count": row_count}]
        )

        results = cmd.raw(toolkit_client_approval.client, "events")

        assert len(results) == 1
        row = results[0]
        assert row[cmd.Columns.Rows] == f"≥{row_count:,}"
        assert row[cmd.Columns.Columns] == f"≥{raw_profile_results_single_column.column_count:,}"
        assert toolkit_client_approval.mock_client.transformations.preview.call_count == 1
