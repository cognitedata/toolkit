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

    yield pytest.param(
        """This PR contains the following updates:

| Package | Change | Age | Adoption | Passing | Confidence |
|---|---|---|---|---|---|
| [certifi](https://redirect.github.com/certifi/python-certifi) | `==2023.11.17` -> `==2024.7.4` | [![age](https://developer.mend.io/api/mc/badges/age/pypi/certifi/2024.7.4?slim=true)](https://docs.renovatebot.com/merge-confidence/) | [![adoption](https://developer.mend.io/api/mc/badges/adoption/pypi/certifi/2024.7.4?slim=true)](https://docs.renovatebot.com/merge-confidence/) | [![passing](https://developer.mend.io/api/mc/badges/compatibility/pypi/certifi/2023.11.17/2024.7.4?slim=true)](https://docs.renovatebot.com/merge-confidence/) | [![confidence](https://developer.mend.io/api/mc/badges/confidence/pypi/certifi/2023.11.17/2024.7.4?slim=true)](https://docs.renovatebot.com/merge-confidence/) |

### GitHub Vulnerability Alerts

#### [CVE-2024-39689](https://redirect.github.com/certifi/python-certifi/security/advisories/GHSA-248v-346w-9cwc)

Certifi 2024.07.04 removes root certificates from "GLOBALTRUST" from the root store. These are in the process of being removed from Mozilla's trust store.

GLOBALTRUST's root certificates are being removed pursuant to an investigation which identified "long-running and unresolved compliance issues". Conclusions of Mozilla's investigation can be found [here]( https://groups.google.com/a/mozilla.org/g/dev-security-policy/c/XpknYMPO8dI).

---

### Release Notes

<details>
<summary>certifi/python-certifi (certifi)</summary>

### [`v2024.7.4`](https://redirect.github.com/certifi/python-certifi/compare/2024.06.02...2024.07.04)

[Compare Source](https://redirect.github.com/certifi/python-certifi/compare/2024.06.02...2024.07.04)

### [`v2024.6.2`](https://redirect.github.com/certifi/python-certifi/compare/2024.02.02...2024.06.02)

[Compare Source](https://redirect.github.com/certifi/python-certifi/compare/2024.02.02...2024.06.02)

### [`v2024.2.2`](https://redirect.github.com/certifi/python-certifi/compare/2023.11.17...2024.02.02)

[Compare Source](https://redirect.github.com/certifi/python-certifi/compare/2023.11.17...2024.02.02)

</details>

---

### Configuration

📅 **Schedule**: Branch creation - "" (UTC), Automerge - At any time (no schedule defined).

🚦 **Automerge**: Disabled by config. Please merge this manually once you are satisfied.

♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the rebase/retry checkbox.

🔕 **Ignore**: Close this PR and you won't be reminded about this update again.

---

 - [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check this box

---

This PR was generated by [Mend Renovate](https://mend.io/renovate/). View the [repository job log](https://developer.mend.io/github/cognitedata/toolkit).
<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiI0MC4xNi4wIiwidXBkYXRlZEluVmVyIjoiNDAuMTYuMCIsInRhcmdldEJyYW5jaCI6Im1haW4iLCJsYWJlbHMiOlsiZGVwZW5kZW5jaWVzIiwicmVub3ZhdGUiXX0=-->""",
        "0.5.16",
        "",
        "0.5.16",
        id="Renovate PR",
    )

    yield pytest.param(
        """fix configuration value for workflows, fix bug with datapoints extractor (#1580)
Note to risk reviewer: the code in this PR is not part of the Toolkit
CLI per se, but rather a sample used by the Academy team when they run
Bootcamps. Hence the requirements for testing and typing etc are a bit
more relaxed.

# Description

Fixed Bootcamp workflows using incorrect credentials, fixed bug
introduced in last change for datapoints extractor.

## Changelog

- [x] Patch
- [ ] Minor
- [ ] Skip

## cdf

No changes

## templates

### Fixed

- Bootcamp config.test.yaml had incorrect credentials for workflow
variables
- fixed bug that would miss list of datapoints when list was < 50""",
        "0.5.16",
        """## cdf

No changes

## templates

### Fixed

- Bootcamp config.test.yaml had incorrect credentials for workflow
variables
- fixed bug that would miss list of datapoints when list was < 50""",
        "0.5.17",
        id="Patch bump with only templates changes",
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

        is_skip = last_version == expected_version
        if is_skip:
            assert actual_changelog_entry is None
        else:
            assert actual_changelog_entry is not None, "Changelog entry was not created"
            removed_trailing_space = "\n".join([line.rstrip() for line in actual_changelog_entry.splitlines()])
            assert removed_trailing_space == expected_changelog

            assert actual_version is not None, "Version was not updated"
            assert actual_version == expected_version
