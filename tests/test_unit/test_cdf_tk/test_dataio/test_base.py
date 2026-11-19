from pathlib import Path

from cognite_toolkit._cdf_tk.dataio._base import UploadableDataIO
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader


class TestUploadableDataIOReadChunks:
    def test_read_chunks_tracking_id_includes_source_file(self, tmp_path: Path) -> None:
        """The tracking ID must identify which physical file an item came from, even when a
        single batch spans a file boundary (i.e. the file is smaller than CHUNK_SIZE)."""
        file1 = tmp_path / "part-0000.ndjson"
        file1.write_text('{"id": 1}\n{"id": 2}\n')
        file2 = tmp_path / "part-0001.ndjson"
        file2.write_text('{"id": 3}\n{"id": 4}\n')
        reader = MultiFileReader([file1, file2])

        pages = list(UploadableDataIO.read_chunks(reader, selector=None))

        assert len(pages) == 1
        tracking_ids = [di.tracking_id for di in pages[0].items]
        assert tracking_ids == [
            "part-0000.ndjson:line-1",
            "part-0000.ndjson:line-2",
            "part-0001.ndjson:line-3",
            "part-0001.ndjson:line-4",
        ]
