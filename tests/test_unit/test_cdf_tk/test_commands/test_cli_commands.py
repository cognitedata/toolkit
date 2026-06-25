import pytest

from cognite_toolkit._cdf_tk.commands._cli_commands import package_install_command, use_uv


class TestPackageInstallCommand:
    def test_package_install_command_with_uv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("UV", "/opt/homebrew/bin/uv")
        assert package_install_command("cognite-neat") == "uv add cognite-neat"

    def test_package_install_command_without_uv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("UV", raising=False)
        assert package_install_command("cognite-neat") == "pip install cognite-neat"

    def test_use_uv_detects_uv_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("UV", "/opt/homebrew/bin/uv")
        assert use_uv() is True

    def test_use_uv_false_without_uv_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("UV", raising=False)
        assert use_uv() is False
