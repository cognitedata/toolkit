"""Unit tests for CommandTrackingInfo data class."""

from cognite_toolkit._cdf_tk.data_classes import CommandTrackingInfo


class TestCommandTrackingInfo:
    """Test CommandTrackingInfo functionality."""

    def test_to_dict_empty(self) -> None:
        """Test that empty CommandTrackingInfo returns empty dict."""
        info = CommandTrackingInfo()
        result = info.to_dict()
        assert result == {}

    def test_to_dict_excludes_default_values(self) -> None:
        """Test that empty sets are excluded from to_dict output."""
        info = CommandTrackingInfo()
        info.module_ids.add("module1")
        result = info.to_dict()
        assert "moduleIds" in result
        assert result["moduleIds"] == ["module1"]
        assert "packageIds" not in result
        assert "installedModuleIds" not in result

    def test_to_dict_includes_all_non_empty_fields(self) -> None:
        """Test that all non-empty fields are included with camelCase aliases."""
        info = CommandTrackingInfo(
            project="test_project",
            cluster="test_cluster",
        )
        info.module_ids.add("module1")
        info.package_ids.add("package1")
        info.installed_module_ids.add("installed1")
        info.downloaded_library_ids.add("lib1")

        result = info.to_dict()

        assert result["project"] == "test_project"
        assert result["cluster"] == "test_cluster"
        assert result["moduleIds"] == ["module1"]
        assert result["packageIds"] == ["package1"]
        assert result["installedModuleIds"] == ["installed1"]
        assert result["downloadedLibraryIds"] == ["lib1"]
        # Empty sets should not be present
        assert "downloadedPackageIds" not in result
        assert "downloadedModuleIds" not in result
        assert "installedPackageIds" not in result
