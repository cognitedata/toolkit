from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import RuleSetVersionId, SemanticVersion


class RuleSetVersion(BaseModelObject):
    """Represents a version of a rule set in CDF."""

    rule_set_external_id: str = Field(exclude=True)
    version: SemanticVersion
    rules: list[str]

    def as_id(self) -> RuleSetVersionId:
        return RuleSetVersionId(
            rule_set_external_id=self.rule_set_external_id,
            version=self.version,
        )


class RuleSetVersionRequest(RuleSetVersion, RequestResource):
    """Request resource for creating rule set versions."""

    pass


class RuleSetVersionResponse(RuleSetVersion, ResponseResource[RuleSetVersionRequest]):
    """Response resource for rule set versions."""

    rule_set_external_id: str = Field(default="", exclude=True)
    created_time: int

    @classmethod
    def request_cls(cls) -> type[RuleSetVersionRequest]:
        return RuleSetVersionRequest

    def as_request_resource(self) -> RuleSetVersionRequest:
        data = self.dump(camel_case=False)
        data["rule_set_external_id"] = self.rule_set_external_id
        return RuleSetVersionRequest.model_validate(data, extra="ignore")
