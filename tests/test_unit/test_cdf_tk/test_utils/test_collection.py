from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.commands._migrate.data_classes import MigrationMapping, MigrationMappingList
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence


class TestChunkSequence:
    def test_chunk_sequence(self):
        """Test that chunker_sequence correctly chunks a sequence."""
        sequence = list(range(10))
        chunk_size = 3
        expected_chunks = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [9],
        ]

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert chunks == expected_chunks

    def test_chunk_sequence_empty(self):
        """Test that chunker_sequence returns an empty list for an empty sequence."""
        sequence = []
        chunk_size = 3

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert chunks == []

    def test_chunk_sequence_maintain_source_type(self) -> None:
        """Test that chunker_sequence maintains the type of the source sequence."""
        sequence = MigrationMappingList(
            [
                MigrationMapping(
                    resourceType="timeseries", id=1, dataSetId=123, instanceId=NodeId("sp_full_ts", "full_ts_id")
                ),
                MigrationMapping(
                    resourceType="timeseries", id=2, dataSetId=None, instanceId=NodeId("sp_step_ts", "step_ts_id")
                ),
            ]
        )
        chunk_size = 1

        chunks = list(chunker_sequence(sequence, chunk_size))
        assert len(chunks) == 2
        assert len(chunks[0]) == 1
        assert len(chunks[1]) == 1
        assert isinstance(chunks[0], MigrationMappingList)
