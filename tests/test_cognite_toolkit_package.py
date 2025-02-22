"""These tests are meant to ensure that the package version is consistent across all files and that the changelog is
properly formatted."""

import re
import sys
from collections.abc import Iterator
from datetime import datetime
from re import Match

import pytest
from packaging.version import Version

from cognite_toolkit._version import __version__
from tests.constants import REPO_ROOT

if sys.version_info >= (3, 11):
    import toml
else:
    import tomli as toml


def test_pyproj_version_matches() -> None:
    version_in_pyproject = toml.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["version"]

    assert __version__ == version_in_pyproject, (
        f"Version in 'pyproject.toml' ({version_in_pyproject}) does not match the version in "
        f"cognite_toolkit/_version.py: ({__version__})"
    )


@pytest.mark.parametrize(
    "package_version, changelog_name",
    [(__version__, "CHANGELOG.cdf-tk.md"), (__version__, "CHANGELOG.templates.md")],
)
def test_changelog_entry_version_matches(package_version: str, changelog_name: str) -> None:
    match = next(_parse_changelog(changelog_name))
    changelog_version = match.group(1)
    assert changelog_version == package_version, (
        f"The latest entry in '{changelog_name}' has a different version ({changelog_version}) than "
        f"cognite_toolkit/_version.py: ({__version__}). Did you forgot to add a new entry? "
        "Or maybe you haven't followed the required format?"
    )


@pytest.mark.parametrize(
    "changelog_name",
    [
        "CHANGELOG.cdf-tk.md",
        "CHANGELOG.templates.md",
    ],
)
def test_version_number_is_increasing(changelog_name: str) -> None:
    versions = [Version(match.group(1)) for match in _parse_changelog(changelog_name)]
    for new, old in zip(versions[:-1], versions[1:]):
        if new < old:
            assert False, f"Versions must be strictly increasing: {new} is not higher than the previous, {old}."
    assert True


@pytest.mark.parametrize(
    "changelog_name",
    [
        "CHANGELOG.cdf-tk.md",
        "CHANGELOG.templates.md",
    ],
)
def test_changelog_entry_date(changelog_name: str) -> None:
    match = next(_parse_changelog(changelog_name))
    try:
        datetime.strptime(date := match.group(3), "%Y-%m-%d")
    except Exception:
        assert False, (
            f"Date given in the newest entry in '{changelog_name}', {date!r}, is not valid/parsable (YYYY-MM-DD)"
        )
    else:
        assert True


def _parse_changelog(changelog: str) -> Iterator[Match[str]]:
    changelog = (REPO_ROOT / changelog).read_text(encoding="utf-8")
    return re.finditer(r"##\s\[(\d+\.\d+\.\d+([ab]\d+)?)\]\s-\s(\d+-\d+-\d+)", changelog)
