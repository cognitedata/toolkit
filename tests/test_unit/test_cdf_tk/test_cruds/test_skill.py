from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.skill import SkillResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, SuccessExtra
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


class TestSkillIO:
    @pytest.mark.parametrize(
        "item, sidecar_builder",
        [
            pytest.param(
                {"externalId": "skill_external"},
                lambda tmp_path, yaml_path: yaml_path.with_suffix(".md"),
                id="candidate-1-yaml-sibling-md",
            ),
            pytest.param(
                {"externalId": "skill_external"},
                lambda tmp_path, yaml_path: tmp_path / "skill_external.Skill.md",
                id="candidate-2-external-id-skill-md",
            ),
        ],
    )
    def test_get_extra_files_loads_each_sidecar_candidate(
        self, tmp_path: Path, item: dict[str, str], sidecar_builder
    ) -> None:
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        sidecar_path = sidecar_builder(tmp_path, yaml_path)
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
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
        assert extras[0].content == _SKILL_CONTENT

    @pytest.mark.parametrize(
        "invalid_content",
        [
            pytest.param("Invalid Markdown", id="invalid-format"),
            pytest.param(
                "---\nname: Invalid Skill Name\ndescription: bad pattern\n---\n\nBody",
                id="invalid-frontmatter-name",
            ),
        ],
    )
    def test_get_extra_files_invalid_markdown_yields_failed_read_extra(
        self, tmp_path: Path, invalid_content: str
    ) -> None:
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        sidecar_path = yaml_path.with_suffix(".md")
        sidecar_path.write_text(invalid_content, encoding="utf-8")
        extras = list(SkillIO.get_extra_files(yaml_path, identifier=ExternalId(external_id="my_skill"), item={}))
        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert extras[0].code == "SYNTAX-ERROR"

    def test_get_extra_files_missing_sidecar_yields_failed_read_extra(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        extras = list(
            SkillIO.get_extra_files(yaml_path, identifier=ExternalId(external_id="my_skill"), item={"externalId": "x"})
        )
        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert extras[0].code == "MISSING"

    def test_get_extra_files_prefers_external_id_specific_sidecar_over_yaml_sibling(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "my.Skill.yaml"
        # Generic sibling sidecar exists but is invalid
        yaml_path.with_suffix(".md").write_text("Invalid Markdown", encoding="utf-8")
        # External-id specific sidecar exists and is valid
        specific_sidecar = tmp_path / "my_other_skill.Skill.md"
        specific_sidecar.write_text(_SKILL_CONTENT, encoding="utf-8")

        extras = list(
            SkillIO.get_extra_files(
                yaml_path,
                identifier=ExternalId(external_id="fallback"),
                item={"externalId": "my_other_skill"},
            )
        )
        assert len(extras) == 1
        assert isinstance(extras[0], SuccessExtra)
        assert extras[0].source_path == specific_sidecar

    def test_split_resource_emits_sidecar_when_missing(self, tmp_path: Path) -> None:
        skill_io = SkillIO(ToolkitClientMock(), None)
        base_yaml = tmp_path / "my.Skill.yaml"
        split = list(
            skill_io.split_resource(
                base_yaml,
                {
                    "externalId": "my_other_skill",
                    "name": "test-skill",
                    "description": "Test skill description",
                    "content": _SKILL_CONTENT,
                },
            )
        )
        assert split[0][0] == tmp_path / "my_other_skill.Skill.md"
        assert split[0][1] == _SKILL_CONTENT
        assert split[1] == (base_yaml, {"externalId": "my_other_skill"})

    def test_split_resource_normalizes_legacy_sibling_sidecar_path(self, tmp_path: Path) -> None:
        skill_io = SkillIO(ToolkitClientMock(), None)
        base_yaml = tmp_path / "my.Skill.yaml"
        existing = base_yaml.with_suffix(".md")
        existing.write_text(_SKILL_CONTENT, encoding="utf-8")
        split = list(
            skill_io.split_resource(
                base_yaml,
                {
                    "externalId": "my_other_skill",
                    "name": "test-skill",
                    "description": "Test skill description",
                    "content": _SKILL_CONTENT,
                },
            )
        )
        assert split[0][0] == tmp_path / "my_other_skill.Skill.md"
        assert split[0][1] == _SKILL_CONTENT
        assert split[1] == (base_yaml, {"externalId": "my_other_skill"})

    def test_split_resource_uses_deterministic_explicit_sidecar_path(self, tmp_path: Path) -> None:
        skill_io = SkillIO(ToolkitClientMock(), None)
        base_yaml = tmp_path / "my.Skill.yaml"
        legacy_sibling = base_yaml.with_suffix(".md")
        legacy_sibling.write_text(_SKILL_CONTENT, encoding="utf-8")
        split = list(
            skill_io.split_resource(
                base_yaml,
                {
                    "externalId": "my_other_skill",
                    "name": "test-skill",
                    "description": "Test skill description",
                    "content": _SKILL_CONTENT,
                },
            )
        )
        assert split[0][0] == tmp_path / "my_other_skill.Skill.md"

    def test_dump_resource_returns_full_request_equivalent_data(self) -> None:
        skill_io = SkillIO(ToolkitClientMock(), None)
        response = SkillResponse(
            external_id="my_skill",
            name="test-skill",
            description="Test skill description",
            content=_SKILL_CONTENT,
            created_time=1,
            last_updated_time=2,
        )
        dumped = skill_io.dump_resource(response, local={"externalId": "my_skill"})
        assert dumped["externalId"] == "my_skill"
        assert dumped["name"] == "test-skill"
        assert dumped["description"] == "Test skill description"
        assert dumped["content"] == _SKILL_CONTENT
