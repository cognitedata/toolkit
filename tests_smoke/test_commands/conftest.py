import pytest

from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


@pytest.fixture
def enable_extend_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.EXTEND_DOWNLOAD)
