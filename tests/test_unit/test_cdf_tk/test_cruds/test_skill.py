from pathlib import Path

from cognite_toolkit._cdf_tk.client.resource_classes.skill import SkillResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import SkillIO

_SKILL_CONTENT = """---
name: test-skill
description: Test skill description
---

# Test Skill

Do the thing.
"""


class TestSkillIODumpResource:
    def test_dump_resource_omits_content_when_stored_in_sidecar(self) -> None:
        client = ToolkitClientMock()
        io = SkillIO(client, None, None)
        local = {
            "externalId": "my_skill",
            "name": "test-skill",
            "description": "Test skill description",
        }
        resource = SkillResponse.model_validate(
            {
                "externalId": "my_skill",
                "name": "test-skill",
                "description": "Test skill description",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "content": _SKILL_CONTENT,
            }
        )

        dumped = io.dump_resource(resource, local)

        assert dumped == local


class TestSkillIOLoadResourceFile:
    def test_load_resource_file_reads_skill_md_sidecar(self, tmp_path: Path) -> None:
        client = ToolkitClientMock()
        io = SkillIO(client, None, None)
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        yaml_path.write_text(
            """externalId: my_skill
name: test-skill
description: Test skill description
"""
        )
        (tmp_path / "my_skill.SKILL.md").write_text(_SKILL_CONTENT)

        resources = io.load_resource_file(yaml_path)

        assert resources[0]["content"] == _SKILL_CONTENT


class TestSkillIOSplitResource:
    def test_split_resource_writes_skill_md_sidecar(self, tmp_path: Path) -> None:
        client = ToolkitClientMock()
        io = SkillIO(client, None, None)
        yaml_path = tmp_path / "my_skill.Skill.yaml"
        resource = {
            "externalId": "my_skill",
            "name": "test-skill",
            "description": "Test skill description",
            "content": _SKILL_CONTENT,
        }

        outputs = dict(io.split_resource(yaml_path, resource))

        assert yaml_path in outputs
        assert outputs[yaml_path] == {
            "externalId": "my_skill",
            "name": "test-skill",
            "description": "Test skill description",
        }
        skill_md_path = tmp_path / "my_skill.Skill.SKILL.md"
        assert skill_md_path in outputs
        assert outputs[skill_md_path] == _SKILL_CONTENT
