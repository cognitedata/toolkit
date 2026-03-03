import builtins
from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import SignalSubscriptionId


class SinkRef(BaseModelObject):
    type: Literal["email", "user", "current_user"]
    external_id: str


class IntegrationsSubscriptionFilter(BaseModelObject):
    topic: Literal["cognite_integrations"]
    resource: str | None = None
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None
    extractor_external_id: str | None = None
    extractor_version: str | None = None


class WorkflowsSubscriptionFilter(BaseModelObject):
    topic: Literal["cognite_workflows"]
    resource: str | None = None
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


SubscriptionFilter = Annotated[
    IntegrationsSubscriptionFilter | WorkflowsSubscriptionFilter,
    Field(discriminator="topic"),
]


class SignalSubscription(BaseModelObject):
    external_id: str

    def as_id(self) -> SignalSubscriptionId:
        return SignalSubscriptionId(external_id=self.external_id)


class SignalSubscriptionRequest(SignalSubscription, UpdatableRequestResource):
    sink: SinkRef
    filter: SubscriptionFilter

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset()

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = self.as_id().dump(camel_case=True)
        filter_value = self.filter.dump(camel_case=True)
        update_item["update"] = {"filter": {"set": filter_value}}
        return update_item


class SignalSubscriptionResponse(SignalSubscription, ResponseResource[SignalSubscriptionRequest]):
    sink: SinkRef
    filter: SubscriptionFilter
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> builtins.type[SignalSubscriptionRequest]:
        return SignalSubscriptionRequest
