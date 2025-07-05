import pytest
from cognite.client.data_classes import (
    Database,
    Table,
    Transformation,
    TransformationDestination,
    TransformationPreviewResult,
)

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
        row_count=ProfileRawCommand.profile_row_limit,
        columns=RawProfileColumns(
            {
                "externalId": StringProfileColumn(
                    count=500,
                    null_count=ProfileRawCommand.profile_row_limit - 500,
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


class TestProfileRawCommand:
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
