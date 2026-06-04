from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class SkillYAML(ToolkitResource):
    """Atlas AI Skill metadata. Skill body content is stored in a sibling `.SKILL.md` file."""

    external_id: str = Field(
        description="An external ID that uniquely identifies the skill.",
        min_length=1,
        max_length=128,
        pattern=r"^[^\x00]{1,128}$",
    )
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

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
