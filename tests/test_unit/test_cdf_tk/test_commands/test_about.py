from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from cognite_toolkit._cdf_tk.commands.about import AboutCommand
from tests.test_unit.utils import PrintCapture


class TestAboutCommand:
    @pytest.mark.parametrize(
        "toml_content, should_warn",
        [
            # Valid configurations (no warnings)
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                """,
                False,
            ),
            (
                """
                [cdf]
                default_env = "dev"
                [modules]
                version = "0.0.0"
                [plugins]
                run = true
                dump = false
                """,
                False,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [alpha_flags]
                graphql = true
                migrate = false
                """,
                False,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.custom]
                url = "https://example.com/packages.zip"
                checksum = "abc123"
                """,
                False,
            ),
            (
                """
                [cdf]
                default_env = "prod"
                [modules]
                version = "0.0.0"
                [plugins]
                run = true
                data = true
                [alpha_flags]
                graphql = true
                [library.my-lib]
                url = "https://example.com/lib.zip"
                checksum = "xyz789"
                """,
                False,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [plugins]
                [alpha_flags]
                """,
                False,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.lib1]
                url = "https://example.com/lib1.zip"
                checksum = "aaa"
                [library.lib2]
                url = "https://example.com/lib2.zip"
                checksum = "bbb"
                """,
                False,
            ),
            # Invalid configurations (should warn)
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [alpha-flags]
                graphql = true
                """,
                True,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [plugin]
                run = true
                """,
                True,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [plugin]
                run = true
                [feature-flags]
                test = true
                """,
                True,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [plugins]
                fake_plugin = true
                run = true
                """,
                True,
            ),
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [alpha_flags]
                fake_flag = true
                graphql = true
                """,
                True,
            ),
        ],
    )
    def test_execute_with_various_configs(
        self,
        tmp_path: Path,
        toml_content: str,
        should_warn: bool,
        capture_print: PrintCapture,
        reset_cdf_toml_singleton,
    ) -> None:
        """Test that about command doesn't crash with various cdf.toml configurations."""
        (tmp_path / "cdf.toml").write_text(dedent(toml_content))

        cmd = AboutCommand(print_warning=False, skip_tracking=True)
        cmd.execute(tmp_path)

        messages = " ".join(capture_print.messages)

        # Verify command produced some output (didn't crash)
        assert len(capture_print.messages) > 0, "Command should produce output"

        # Verify warning appears if expected
        if should_warn:
            assert "Warning" in messages, "Expected warning in output for invalid config"

    def test_execute_without_cdf_toml_searches_subdirectories(
        self, tmp_path: Path, capture_print: PrintCapture, reset_cdf_toml_singleton
    ) -> None:
        """Test that about command finds cdf.toml in subdirectories."""
        subdir = tmp_path / "project"
        subdir.mkdir()
        (subdir / "cdf.toml").write_text(
            dedent(
                """
                [cdf]
                [modules]
                version = "0.0.0"
                """
            )
        )

        cmd = AboutCommand(print_warning=False, skip_tracking=True)
        cmd.execute(tmp_path)

        messages = " ".join(capture_print.messages)
        assert "Cognite Toolkit" in messages
        assert "Found" in messages or "subdirectories" in messages.lower()


class TestFindSimilarTable:
    @pytest.mark.parametrize(
        "unrecognized, expected_suggestion",
        [
            # Hyphen variants
            ("alpha-flags", "alpha_flags"),
            ("feature-flags", "feature_flags"),
            # Missing 's'
            ("plugin", "plugins"),
            # Case variations
            ("AlphaFlags", "alpha_flags"),
            ("PLUGINS", "plugins"),
            # No match
            ("unknown-table", None),
            ("random", None),
            # Valid table (should not suggest itself)
            ("library", None),
        ],
    )
    def test_find_similar_table(self, unrecognized: str, expected_suggestion: str | None) -> None:
        """Test _find_similar_table with various table name variations."""
        cmd = AboutCommand(print_warning=False, skip_tracking=True)
        valid_tables = {"cdf", "modules", "alpha_flags", "feature_flags", "plugins", "library"}

        result = cmd._find_similar_table(unrecognized, valid_tables)

        assert result == expected_suggestion
