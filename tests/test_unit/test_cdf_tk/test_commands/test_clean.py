from pathlib import Path
from unittest.mock import patch

import pytest

# For other flags, use the real implementation
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import BuildCommand, CleanCommand
from cognite_toolkit._cdf_tk.constants import clean_name
from cognite_toolkit._cdf_tk.data_classes import BuiltModuleList
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture
def built_modules(
    build_tmp_path: Path,
    complete_org_dir: Path,
    env_vars_with_client: EnvironmentVariables,
) -> Path:
    """Build modules from complete_org_dir and return the build directory."""
    return BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=complete_org_dir,
        build_dir=build_tmp_path,
        selected=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        build_env_name="dev",
        on_error="raise",
    )


class TestCleanCommandExecute:
    """Test the CleanCommand.execute method."""

    def test_clean_without_v07_flag_and_include_none_deletes_all_modules(
        self,
        built_modules: BuiltModuleList,
        build_tmp_path: Path,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
    ) -> None:
        """Test that when Flags.v07 is disabled and include=None, all modules are deleted."""

        # Track expected resource kinds that should have delete calls with their counts
        # When Flags.v07 is disabled, all modules are cleaned, so we expect delete calls
        # for resources (excluding data_sets which don't support deletion)
        expected_resources_to_be_deleted: dict[str, int] = {}
        for module in built_modules:
            for resource_folder, resource_list in module.resources.items():
                for resource in resource_list:
                    kind = resource.kind
                    expected_resources_to_be_deleted[kind] = expected_resources_to_be_deleted.get(kind, 0) + 1

        # Mock FeatureFlag.is_enabled() to return False for Flags.v07, but respect settings for other flags
        def v07_disabled_side_effect(flag: Flags) -> bool:
            if flag == Flags.v07:
                return False
            return CDFToml.load().alpha_flags.get(clean_name(flag.name), False)

        with patch.object(FeatureFlag, "is_enabled", side_effect=v07_disabled_side_effect):
            CleanCommand(silent=True, skip_tracking=True).execute(
                env_vars=env_vars_with_client,
                build_dir=build_tmp_path,
                build_env_name="dev",
                dry_run=False,
                include=None,
                module_str=None,
                verbose=False,
            )

        # Verify that delete calls were made for the expected resource kinds
        delete_calls = toolkit_client_approval.delete_calls()

        # Verify that delete calls were made for expected resource kinds
        # The delete_calls dict has API names as keys (like "iam" for groups)
        # We expect at least some delete calls when Flags.v07 is disabled and include=None
        assert expected_resources_to_be_deleted == delete_calls

        # Check that no unmocked calls were made
        not_mocked = toolkit_client_approval.not_mocked_calls()
        assert not not_mocked, (
            f"The following APIs have been called without being mocked: {not_mocked}, "
            "Please update the list _API_RESOURCES in tests/approval_client.py"
        )
