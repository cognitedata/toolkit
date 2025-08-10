from pathlib import Path

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import RawIO


class TestUploadCommand:
    def test_upload_raw_rows(self, tmp_path: Path) -> None:
        cmd = UploadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            cmd.upload(RawIO(client), tmp_path, verbose=False)
