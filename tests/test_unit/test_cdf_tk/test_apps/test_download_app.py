from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps._download_app import AssetCentricFormats, DownloadApp, _prompt_file_format
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags

MODULE = "cognite_toolkit._cdf_tk.apps._download_app"


def _enable_extend_download(monkeypatch: pytest.MonkeyPatch, enabled: bool) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: enabled and flag is Flags.EXTEND_DOWNLOAD)


class TestDownloadAppSQLite:
    @pytest.mark.parametrize(
        "arguments",
        [
            ["raw", "my_table", "--database", "my_db", "--format", "sqlite"],
            ["assets", "--data-set", "my_data_set", "--format", "sqlite"],
            ["instances", "--instance-space", "my_space", "--format", "sqlite"],
            ["datapoints", "my_data_set", "--format", "sqlite"],
        ],
    )
    def test_sqlite_requires_alpha_flag(self, arguments: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
        _enable_extend_download(monkeypatch, enabled=False)

        result = CliRunner().invoke(DownloadApp(), arguments, env={"COLUMNS": "200"})

        assert (result.exit_code, "extend_download" in click.unstyle(result.output)) == (2, True)

    def test_sqlite_forwarded_to_download_command(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _enable_extend_download(monkeypatch, enabled=True)
        command = MagicMock()
        command.run.side_effect = lambda callback: callback()

        with (
            patch(f"{MODULE}.EnvironmentVariables"),
            patch(f"{MODULE}.DownloadCommand", return_value=command),
        ):
            result = CliRunner().invoke(DownloadApp(), ["assets", "--data-set", "my_data_set", "--format", "sqlite"])

        assert (result.exit_code, command.download.call_args.kwargs["file_format"]) == (0, ".sqlite")

    @pytest.mark.parametrize("enabled", [True, False])
    def test_prompt_hides_sqlite_without_alpha_flag(self, enabled: bool, monkeypatch: pytest.MonkeyPatch) -> None:
        _enable_extend_download(monkeypatch, enabled)

        with patch(f"{MODULE}.questionary.select") as select:
            _prompt_file_format("assets", AssetCentricFormats, AssetCentricFormats.csv)

        choices = [choice.value for choice in select.call_args.kwargs["choices"]]
        assert (AssetCentricFormats.sqlite in choices) is enabled
