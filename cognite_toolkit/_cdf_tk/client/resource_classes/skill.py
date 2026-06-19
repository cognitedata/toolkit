from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class Skill(BaseModelObject):
    external_id: str
    name: str
    description: str


class SkillRequest(Skill, RequestResource):
    content: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SkillResponse(Skill, ResponseResource[SkillRequest]):
    created_time: int | None = None
    last_updated_time: int | None = None
    content: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    @classmethod
    def request_cls(cls) -> type[SkillRequest]:
        return SkillRequest
