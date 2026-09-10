from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.auth.session_keyring import configure_sample_store, reset_store


@pytest.fixture
def sample_keyring(tmp_path: Path):
    backing_file = tmp_path / "keyring.ron"
    configure_sample_store(str(backing_file))
    yield backing_file
    reset_store()
