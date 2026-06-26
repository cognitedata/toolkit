from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.rules._neat import NeatRuleSet


class TestNeatRuleSetStatus:
    @patch.object(NeatRuleSet, "installed", return_value=True)
    def test_get_status_unavailable_when_installed_without_client(self, _installed: object) -> None:
        status = NeatRuleSet(modules=[]).get_status()

        assert status.code == "unavailable"
        assert status.message is not None
        assert "Neat is installed" in status.message
        assert "cdf auth init" in status.message

    @patch.object(NeatRuleSet, "installed", return_value=True)
    def test_get_status_ready_when_installed_with_client(self, _installed: object) -> None:
        status = NeatRuleSet(modules=[], client=MagicMock()).get_status()

        assert status.code == "ready"
        assert status.message is not None
        assert "Neat is installed" in status.message

    @patch.object(NeatRuleSet, "installed", return_value=False)
    def test_get_status_suggests_uv_when_running_under_uv(
        self, _installed: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("UV", "/opt/homebrew/bin/uv")
        status = NeatRuleSet(modules=[]).get_status()

        assert status.code == "unavailable"
        assert status.message is not None
        assert "uv add cognite-neat" in status.message
        assert "pip install cognite-neat" not in status.message

    @patch.object(NeatRuleSet, "installed", return_value=False)
    def test_get_status_suggests_pip_without_uv(self, _installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("UV", raising=False)
        status = NeatRuleSet(modules=[]).get_status()

        assert status.code == "unavailable"
        assert status.message is not None
        assert "pip install cognite-neat" in status.message
        assert "uv add cognite-neat" not in status.message
