from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf_tk.apps import ApiApp, AuthApp, InitApp
from cognite_toolkit._cdf_tk.commands.auth import AuthCommand, VerifyAuthResult
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


@pytest.fixture
def v09_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    original = FeatureFlag.is_enabled

    monkeypatch.setattr(
        FeatureFlag,
        "is_enabled",
        lambda flag: flag is Flags.V09 or original(flag),
    )


class TestV09CliRegistration:
    def test_api_functions_activate_help(self, v09_enabled: None) -> None:
        result = CliRunner().invoke(ApiApp(), ["functions", "activate", "--help"])
        assert result.exit_code == 0
        assert "Activate the CDF Function service" in result.output

    def test_init_subcommands_registered(self, v09_enabled: None) -> None:
        app = InitApp()
        result = CliRunner().invoke(app, ["auth", "--help"])
        assert result.exit_code == 0
        assert "Configure project credentials" in result.output

        result = CliRunner().invoke(app, ["access", "--help"])
        assert result.exit_code == 0
        assert "Set up toolkit group" in result.output

    def test_auth_status_registered(self, v09_enabled: None) -> None:
        app = AuthApp()
        result = CliRunner().invoke(app, ["status", "--help"])
        assert result.exit_code == 0
        assert "project access status" in result.output

    def test_provision_access_does_not_call_activate_function_service(self, v09_enabled: None) -> None:
        cmd = AuthCommand(print_warning=False, skip_tracking=True)
        client = MagicMock()
        with (
            patch.object(cmd, "_prepare_access_context") as mock_context,
            patch.object(cmd, "activate_function_service") as mock_activate,
            patch.object(cmd, "_create_toolkit_group_in_cdf_interactive", return_value=None),
        ):
            mock_context.return_value = MagicMock(
                toolkit_group=MagicMock(name="cdf-toolkit-group"),
                is_user_in_toolkit_group=False,
                is_toolkit_group_existing=False,
                cdf_toolkit_group=None,
                user_groups=[],
                all_groups=[],
                cdf_project="test-project",
                data_modeling_status="HIDDEN",
                resource_names_by_acl_type={},
            )
            cmd.provision_access(client, dry_run=True, no_prompt=True)

        mock_activate.assert_not_called()

    def test_verify_emits_deprecation_when_v09(self, v09_enabled: None) -> None:
        cmd = AuthCommand(print_warning=False, skip_tracking=True)
        client = MagicMock()
        with (
            patch.object(cmd, "audit_access"),
            patch.object(cmd, "provision_access", return_value=VerifyAuthResult()),
            patch.object(cmd, "activate_function_service", return_value=None),
            patch("cognite_toolkit._cdf_tk.commands.auth.ToolkitDeprecationWarning.print_warning") as mock_warn,
        ):
            cmd.verify(client, dry_run=True, no_prompt=True)

        mock_warn.assert_called_once()
