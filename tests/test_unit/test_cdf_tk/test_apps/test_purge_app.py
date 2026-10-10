from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.apps._purge import PurgeApp
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def _mock_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch_toolkit_client() as client:
        client.tool.token.verify_acls.return_value = []
        environment = MagicMock()
        environment.get_client.return_value = client
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.apps._purge.EnvironmentVariables.create_from_environment",
            lambda: environment,
        )


class TestPurgeInteractiveOptions:
    def test_interactive_instances_rejects_instance_space(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_instances(view=None, instance_space=["SPACE_X"])

        assert str(exc_info.value) == (
            "Cannot specify --instance-space when running in interactive mode. Please omit the --instance-space option."
        )

    def test_interactive_dataset_rejects_ignored_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_dataset(MagicMock(), external_id=None, skip_data=True, asset_recursive=True)

        assert (
            str(exc_info.value) == "Cannot specify --skip-data, --asset-recursive when running in interactive mode. "
            "Please omit these options."
        )

    def test_interactive_space_rejects_ignored_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_environment(monkeypatch)
        with pytest.raises(ToolkitValueError) as exc_info:
            PurgeApp.purge_space(MagicMock(), space=None, include_space=True, dry_run=True)

        assert (
            str(exc_info.value) == "Cannot specify --include-space, --dry-run when running in interactive mode. "
            "Please omit these options."
        )
