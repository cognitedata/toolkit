import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import (
    SequenceColumnRequest,
    SequenceRequest,
    SequenceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import (
    SequenceRow,
    SequenceRowsRequest,
)
from cognite_toolkit._cdf_tk.cruds import SequenceRowCRUD

SEQUENCE_EXTERNAL_ID = "toolkit_smoke_test_large_sequence"


@pytest.fixture(scope="session")
def large_sequence(toolkit_client: ToolkitClient) -> SequenceResponse:
    sequence_request = SequenceRequest(
        external_id=SEQUENCE_EXTERNAL_ID, columns=[SequenceColumnRequest(external_id="col1")]
    )
    retrieved = toolkit_client.tool.sequences.retrieve([sequence_request.as_id()], ignore_unknown_ids=True)
    if len(retrieved) == 0:
        return toolkit_client.tool.sequences.create([sequence_request])[0]
    return retrieved[0]


class TestSequenceRowCRUD:
    def test_create_delete_large_sequence(
        self, toolkit_client: ToolkitClient, large_sequence: SequenceResponse
    ) -> None:
        count = 10_000
        many_rows = SequenceRowsRequest(
            external_id=SEQUENCE_EXTERNAL_ID,
            columns=[large_sequence.columns[0].external_id],
            rows=[SequenceRow(row_number=no, values=[1000.0 + no]) for no in range(count)],
        )
        io = SequenceRowCRUD(toolkit_client, None, None)

        try:
            io.create([many_rows])
        except ToolkitAPIError as e:
            raise AssertionError(f"Failed inserting {count} rows into sequence. Got error: {e}")

        try:
            deleted = io.delete([large_sequence.as_id()])
        except ToolkitAPIError as e:
            raise AssertionError(f"Failed deleting rows from sequence. Got error: {e}")
        if deleted != count:
            raise AssertionError(f"Expected to delete 1 sequence, but deleted {deleted}")
