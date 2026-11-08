import json
from pathlib import Path
from typing import Literal

import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import (
    SequenceColumnResponse,
    SequenceColumnSlim,
    SequenceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import SequenceRow, SequenceRowsResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.dataio import SequenceRowIO
from cognite_toolkit._cdf_tk.dataio.selectors import SequenceRowSelector
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader

COLUMNS: list[tuple[str, Literal["STRING", "DOUBLE", "LONG"]]] = [
    ("pressure", "DOUBLE"),
    ("count", "LONG"),
    ("label", "STRING"),
]


@pytest.fixture()
def sequence() -> SequenceResponse:
    return SequenceResponse(
        id=1,
        external_id="my_sequence",
        created_time=0,
        last_updated_time=0,
        columns=[
            SequenceColumnResponse(external_id=name, value_type=value_type, created_time=0, last_updated_time=0)
            for name, value_type in COLUMNS
        ],
    )


@pytest.fixture()
def sequence_rows() -> SequenceRowsResponse:
    return SequenceRowsResponse(
        id=1,
        external_id="my_sequence",
        columns=[SequenceColumnSlim(external_id=name, value_type=value_type) for name, value_type in COLUMNS],
        rows=[
            *(SequenceRow(row_number=no, values=[no * 1.5, no, f"label_{no}"]) for no in range(1, 6)),
            SequenceRow(row_number=6, values=[None, None, None]),
        ],
    )


class TestSequenceRowIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    @pytest.mark.parametrize("file_format", [".ndjson", ".csv", ".parquet"])
    def test_download_upload_round_trip(
        self,
        file_format: str,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        tmp_path: Path,
        sequence: SequenceResponse,
        sequence_rows: SequenceRowsResponse,
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/sequences/data")).respond(status_code=200, json={})
        selector = SequenceRowSelector(external_id="my_sequence", download_dir_name="sequences")
        with monkeypatch_toolkit_client() as client:
            client.config = toolkit_config
            client.sequences.retrieve.return_value = [sequence]
            client.sequences.rows.paginate.return_value = PagedResponse(items=[sequence_rows], nextCursor=None)

            DownloadCommand(silent=True, skip_tracking=True).download(
                selectors=[selector],
                io=SequenceRowIO(client),
                output_dir=tmp_path,
                verbose=False,
                file_format=file_format,
                compression="none",
                limit=None,
            )
            UploadCommand(silent=True, skip_tracking=True).upload(
                tmp_path / "sequences", client, deploy_resources=False, dry_run=False, verbose=False
            )

        uploaded = [item for call in respx_mock.calls for item in json.loads(call.request.content)["items"]]
        config_dir = tmp_path / "sequences" / DATA_RESOURCE_DIR / "classic"
        assert {
            "uploaded": uploaded,
            "configs": sorted(file.name for file in config_dir.iterdir()),
        } == {
            "uploaded": [sequence_rows.as_request_resource().dump()],
            "configs": ["my_sequence.Sequence.yaml"],
        }

    def test_read_chunks_splits_batches_by_value_limit(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        csv_file = tmp_path / f"my_sequence.{SequenceRowIO.KIND}.csv"
        csv_file.write_text("rowNumber,a,b\n" + "".join(f"{no},{no},{no}\n" for no in range(1, 6)))
        monkeypatch.setattr(SequenceRowIO, "MAX_VALUES_PER_REQUEST", 4)

        pages = SequenceRowIO.read_chunks(MultiFileReader([csv_file]), SequenceRowSelector(external_id="my_sequence"))

        assert [item.tracking_id for page in pages for item in page.items] == [
            "my_sequence.SequenceRows.csv:rows-1-2",
            "my_sequence.SequenceRows.csv:rows-3-4",
            "my_sequence.SequenceRows.csv:rows-5-5",
        ]
