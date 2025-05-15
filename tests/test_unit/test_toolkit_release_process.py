"""These tests are for the dev.py CLI used to create the bump and changelog entry"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import dev


def get_release_process_test_cases():
    yield pytest.param(
        """# Description

Use the new `pydantic` class to replace the `get_paramter_spec` for giving error message. So far we only support `TimeSeries`.

## Changelog

- [X] Patch
- [ ] Minor
- [ ] Skip

## cdf

### Improved

- Improved warnings if a `TimeSeries` configuration is not following the expected spec.

## templates

No changes.""",
        "0.5.15",
        """## cdf

### Improved

- Improved warnings if a `TimeSeries` configuration is not following the expected spec.

## templates

No changes.""",
        "0.5.16",
        id="Patch bump",
    )

    yield pytest.param(
        """# Description

Release `0.5.0`. This is done by removing the alpha flags for the 4 features that are planned for v5.

## Changelog

- [ ] Patch
- [x] Minor
- [ ] Skip

## cdf

### Changed

- The `cdf dump datamodel` no longer includes data models that are global (in system space). You can get the original behavior by using the `--include-global` flag.
- Toolkit now stores a hash of the credentials of Workflow/Transformation/Function in the resources such that the resource is updated when the credentials change.
- For Workflow/Transformations/Function Toolkit no longer falls back to Toolkit credentials when validation-type != 'dev' in the `config.[env].yaml`.

### Added
- Toolkit now support for `cdf dump workflow/transformation/group/node`.

## templates

No changes.
---------

Co-authored-by: Member <member@users.noreply.github.com>
Co-authored-by: Member <member@cognite.com>""",
        "0.4.39",
        """## cdf

### Changed

- The `cdf dump datamodel` no longer includes data models that are global (in system space). You can get the original behavior by using the `--include-global` flag.
- Toolkit now stores a hash of the credentials of Workflow/Transformation/Function in the resources such that the resource is updated when the credentials change.
- For Workflow/Transformations/Function Toolkit no longer falls back to Toolkit credentials when validation-type != 'dev' in the `config.[env].yaml`.

### Added
- Toolkit now support for `cdf dump workflow/transformation/group/node`.

## templates

No changes.""",
        "0.5.0",
        id="Minor bump with co-authors",
    )


class TestReleaseProcess:
    @pytest.mark.parametrize(
        "last_git_message, last_version, expected_changelog, expected_version", list(get_release_process_test_cases())
    )
    def test_bump_and_create_changelog_entry(
        self, last_git_message: str, last_version: str, expected_changelog: str, expected_version: str, monkeypatch
    ) -> None:
        actual_changelog_entry: str | None = None
        actual_version: str | None = None
        last_git_message_file = MagicMock(spec=Path)
        last_git_message_file.read_text.return_value = last_git_message
        last_version_file = MagicMock(spec=Path)
        last_version_file.read_text.return_value = last_version

        def mock_write_changelog(content, encoding=None):
            nonlocal actual_changelog_entry
            actual_changelog_entry = content

        changelog_file = MagicMock(spec=Path)
        changelog_file.write_text = mock_write_changelog

        version_file = MagicMock(spec=Path)
        version_file.read_text.return_value = dev.VERSION_PLACEHOLDER

        def mock_write_version(content, **_):
            nonlocal actual_version
            actual_version = content

        version_file.write_text = mock_write_version

        monkeypatch.setattr(dev, "LAST_GIT_MESSAGE_FILE", last_git_message_file)
        monkeypatch.setattr(dev, "LAST_VERSION", last_version_file)
        monkeypatch.setattr(dev, "CHANGELOG_ENTRY_FILE", changelog_file)
        monkeypatch.setattr(dev, "VERSION_FILES", [version_file])
        monkeypatch.setattr(dev, "DOCKER_IMAGE_FILES", [])

        dev.bump()
        dev.create_changelog_entry()

        assert actual_changelog_entry is not None, "Changelog entry was not created"
        removed_trailing_space = "\n".join([line.rstrip() for line in actual_changelog_entry.splitlines()])
        assert removed_trailing_space == expected_changelog

        assert actual_version is not None, "Version was not updated"
        assert actual_version == expected_version
