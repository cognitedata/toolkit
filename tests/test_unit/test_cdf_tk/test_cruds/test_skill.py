from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._base_ios import SuccessExtra
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import Markdown, SkillIO

_SKILL_CONTENT = """---
name: test-skill
description: Test skill description
---

# Test Skill

Do the thing.

```python
print("Hello, world!")
```

"""


class TestSkillMarkdownParser:
    def test_valid_markdown(self) -> None:
        skill_markdown = Markdown.from_markdown(_SKILL_CONTENT)
        assert skill_markdown.name == "test-skill"
        assert skill_markdown.description == "Test skill description"
        assert skill_markdown.content is not None
        assert skill_markdown.content == _SKILL_CONTENT

    def test_invalid_frontmatter(self) -> None:
        raw = "---\nInvalid YAML frontmatter\n---\nValid Markdown"
        skill_markdown = Markdown.from_markdown(raw)
        assert skill_markdown.name is None
        assert skill_markdown.description is None
        assert skill_markdown.content == raw

    def test_invalid_skill_md_format(self) -> None:
        skill_markdown = Markdown.from_markdown("Invalid Markdown")
        assert skill_markdown.name is None
        assert skill_markdown.description is None
        assert skill_markdown.content is None


class TestSkillIO:
    def test_load_resource_file_with_inline_content(self, tmp_path: Path) -> None:
        skill_io = SkillIO(ToolkitClientMock(), None)
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        yaml_path.write_text(
            """externalId: my_skill
name: test-skill
description: Test skill description
content: |
  ---
  name: test-skill
  description: Test skill description
  ---

  # Test Skill

  Do the thing.
""",
            encoding="utf-8",
        )

        res = skill_io.load_resource_file(yaml_path)
        assert len(res) == 1
        assert res[0]["externalId"] == "my_skill"
        assert res[0]["name"] == "test-skill"
        assert res[0]["description"] == "Test skill description"
        assert "Do the thing." in res[0]["content"]

    @pytest.mark.parametrize(
        "item, sidecar_name",
        [
            pytest.param({}, "my_skill.Skill.md", id="missing external_id: uses yaml prefix"),
            pytest.param(
                {"externalId": "skill_external"},
                "skill_external.Skill.md",
                id="has external_id: uses external_id prefix",
            ),
            pytest.param(
                {"externalId": "skill_external"},
                "my_skill.Skill.md",
                id="has external_id: still picks yaml prefix first",
            ),
        ],
    )
    def test_get_extra_files_sidecar_resolution(self, tmp_path: Path, item: dict[str, str], sidecar_name: str) -> None:
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        sidecar_path = tmp_path / sidecar_name
        sidecar_path.write_text(_SKILL_CONTENT, encoding="utf-8")

        extras = list(
            SkillIO.get_extra_files(
                yaml_path,
                identifier=ExternalId(external_id="fallback_id"),
                item=item,
            )
        )
        assert len(extras) == 1
        assert isinstance(extras[0], SuccessExtra)
        assert extras[0].source_path.resolve().as_posix().lower() == sidecar_path.resolve().as_posix().lower()
