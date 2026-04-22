"""Unit tests for CommandTrackingInfo data class."""

from cognite_toolkit._cdf_tk.data_classes import CommandTracking


class TestCommandTrackingInfo:
    """Test CommandTrackingInfo functionality."""

    def test_to_dict_excludes_default_values(self) -> None:
        """Test that empty sets are excluded from to_dict output."""
        info = CommandTracking(event_name="command37")
        info.module_ids.add("module1")
        assert info.to_dict() == {"moduleIds": ["module1"]}

    def test_to_dict_includes_all_non_empty_fields(self) -> None:
        """Test that all non-empty fields are included with camelCase aliases."""
        info = CommandTracking(event_name="command37")
        info.module_ids.add("module1")
        info.package_ids.add("package1")
        info.installed_module_ids.add("installed1")
        info.downloaded_library_ids.add("lib1")

        assert info.to_dict() == {
            "moduleIds": ["module1"],
            "packageIds": ["package1"],
            "installedModuleIds": ["installed1"],
            "downloadedLibraryIds": ["lib1"],
        }
