import re

import yaml
from pydantic import BaseModel, Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource

SKILL_MD_CONTENT_RE = re.compile(r"^(?:\ufeff)?---\s*\n([\s\S]*?)\n---\s*\n([\s\S]*\S[\s\S]*)$")


class SkillMarkdown(BaseModel):
    name: str = Field(
        description="The name of the skill.",
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    )
    description: str = Field(
        description="Description of what the skill does and when to use it.",
        min_length=1,
        max_length=1024,
    )
    content: str | None = Field(
        default=None,
        description="Skill content in SKILL.md form: YAML frontmatter (between --- lines) followed by a Markdown body. The frontmatter must be a YAML object that includes name and description keys whose values match the top-level name and description fields on this request.",
        pattern=SKILL_MD_CONTENT_RE,
    )

    @classmethod
    def load(cls, raw: str) -> "SkillMarkdown":
        match = SKILL_MD_CONTENT_RE.match(raw)
        if match:
            frontmatter = yaml.safe_load(match.group(1))
            if not isinstance(frontmatter, dict):
                raise ValueError("Invalid markdown frontmatter: expected YAML mapping")
            name = frontmatter.get("name")
            description = frontmatter.get("description")
            if not isinstance(name, str) or not isinstance(description, str):
                raise ValueError("Invalid markdown frontmatter: name and description must be strings")
            return cls(name=name, description=description, content=raw)
        raise ValueError(f"Invalid markdown: {raw}")

    @classmethod
    def dump(cls, name: str, description: str, content: str) -> str:
        return f"---\nname: {name}\ndescription: {description}\n---\n{content}"


class SkillYAML(ToolkitResource):
    """Atlas AI Skill metadata. Skill body content is stored in a sibling ``.SKILL.md`` file."""

    external_id: str = Field(
        description="An external ID that uniquely identifies the skill.",
        min_length=1,
        max_length=128,
        pattern=r"^[^\x00]{1,128}$",
    )
    content: str | None = Field(
        default=None,
        description="Optional inline SKILL.md content. If omitted, content is expected from a sidecar markdown file.",
        pattern=SKILL_MD_CONTENT_RE,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
