#!/usr/bin/env python3
"""Test script to verify BuildLineage implementation."""

from pathlib import Path

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildConfigLineage,
    BuildLineage,
    ModuleLineageItem,
)


def test_imports():
    """Test all classes can be imported."""
    print("✓ All lineage classes imported successfully")


def test_build_config_lineage():
    """Test BuildConfigLineage instantiation."""
    config = BuildConfigLineage(
        organization_dir=Path("/tmp/org"),
        build_dir=Path("/tmp/build"),
        config_name=None,
        cdf_project="my-project",
        validation_type="prod",
        selected_modules={"module1", "module2"},
    )
    assert config.cdf_project == "my-project"
    assert config.validation_type == "prod"
    print("✓ BuildConfigLineage works correctly")


def test_build_lineage():
    """Test BuildLineage instantiation."""
    config = BuildConfigLineage(
        organization_dir=Path("/tmp/org"),
        build_dir=Path("/tmp/build"),
        cdf_project="my-project",
        validation_type="prod",
        selected_modules={"module1", "module2"},
    )

    lineage = BuildLineage(
        config_lineage=config,
        total_modules=2,
        total_modules_processed=2,
        total_resources_discovered=10,
        total_resources_built=10,
        total_syntax_errors=0,
        total_consistency_errors=0,
        total_warnings=1,
        total_recommendations=2,
        build_successful=True,
    )

    assert lineage.build_successful is True
    assert lineage.total_modules == 2
    assert lineage.total_warnings == 1
    print("✓ BuildLineage instantiation works correctly")


def test_build_report():
    """Test build report generation."""
    config = BuildConfigLineage(
        organization_dir=Path("/tmp/org"),
        build_dir=Path("/tmp/build"),
        cdf_project="my-project",
        validation_type="prod",
        selected_modules={"module1"},
    )

    lineage = BuildLineage(
        config_lineage=config,
        total_modules=1,
        total_modules_processed=1,
        total_resources_discovered=5,
        total_resources_built=5,
        total_syntax_errors=0,
        total_consistency_errors=0,
        total_warnings=0,
        total_recommendations=0,
        build_successful=True,
    )

    report = lineage.build_report
    assert "timestamp" in report
    assert "duration_seconds" in report
    assert "modules" in report
    assert "resources" in report
    assert "dependencies" in report
    assert "insights" in report
    assert report["overall_status"] == "SUCCESS"
    print("✓ Build report generation works correctly")


def test_module_lineage():
    """Test ModuleLineageItem instantiation."""
    import tempfile

    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModuleSource

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as tmpdir:
        module_dir = Path(tmpdir) / "module1"
        module_dir.mkdir()

        module_source = ModuleSource(
            path=module_dir,
            id=Path("module1"),
        )

        module_lineage = ModuleLineageItem(
            module_source=module_source,
            iteration=0,
            discovered_resources=3,
            parsing_successful=True,
            parsing_errors_count=0,
            built_resources_count=3,
            total_insights_count=0,
        )

        assert module_lineage.overall_status == "SUCCESS"
        assert module_lineage.built_resources_count == 3
        print("✓ ModuleLineageItem works correctly")


if __name__ == "__main__":
    print("Testing BuildLineage implementation...\n")
    test_imports()
    test_build_config_lineage()
    test_build_lineage()
    test_build_report()
    test_module_lineage()
    print("\n✓ All tests passed! BuildLineage implementation is working correctly.")
