from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import RuleSetVersionId, SemanticVersion

from .base import ToolkitResource


class RuleSetVersionYAML(ToolkitResource):
    rule_set_external_id: str = Field(
        description="External ID of the parent rule set.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    version: SemanticVersion = Field(
        description="Semantic version of this rule set version (major.minor.patch).",
    )
    rules: list[str] | None = Field(
        default=None,
        description="Inline SHACL rules in Turtle format. Mutually exclusive with rulesFile.",
        min_length=1,
        max_length=100,
    )
    rules_file: str | None = Field(
        default=None,
        alias="rulesFile",
        description="Used by Toolkit: path to a .ttl file containing SHACL rules in Turtle format. Mutually exclusive with rules.",
    )

    def as_id(self) -> RuleSetVersionId:
        return RuleSetVersionId(
            rule_set_external_id=self.rule_set_external_id,
            version=self.version,
        )
