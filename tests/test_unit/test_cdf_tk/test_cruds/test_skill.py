from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import SkillIO

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

        skill_dict = SkillIO._parse_markdown(_SKILL_CONTENT)
        assert skill_dict is not None
        assert skill_dict.get("name") == "test-skill"
        assert skill_dict.get("description") == "Test skill description"
        assert skill_dict.get("content") is not None
        assert skill_dict.get("content").startswith("# Test Skill\n\nDo the thing.")

    def test_invalid_frontmatter(self) -> None:

        skill_dict = SkillIO._parse_markdown("---\nInvalid YAML frontmatter\n---\nValid Markdown")
        assert skill_dict is None

    def test_invalid_skill_md_format(self) -> None:
        skill_dict = SkillIO._parse_markdown("Invalid Markdown")
        assert skill_dict is None
