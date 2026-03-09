import builtins
from typing import Annotated, Any, ClassVar, Literal

from pydantic import BeforeValidator, ConfigDict, Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import SignalSubscriptionId
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class SinkRefWithExternalId(BaseModelObject):
    type: Literal["email", "user"]
    external_id: str


class CurrentUserSinkRef(BaseModelObject):
    type: Literal["current_user"] = "current_user"


SinkRef = Annotated[
    SinkRefWithExternalId | CurrentUserSinkRef,
    Field(discriminator="type"),
]


class SubscriptionFilterBase(BaseModelObject):
    model_config = ConfigDict(extra="allow")
    topic: str


class IntegrationsSubscriptionFilter(SubscriptionFilterBase):
    topic: Literal["cognite_integrations"]
    resource: str | None = None
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None
    extractor_external_id: str | None = None
    extractor_version: str | None = None


class WorkflowsSubscriptionFilter(SubscriptionFilterBase):
    topic: Literal["cognite_workflows"]
    resource: str | None = None
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


class HostedExtractorsSubscriptionFilter(SubscriptionFilterBase):
    topic: Literal["cognite_hosted_extractors"]
    resource: str | None = None
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


class UnknownSubscriptionFilter(SubscriptionFilterBase): ...


_KNOWN_FILTERS: dict[str, type[SubscriptionFilterBase]] = {
    cls_.model_fields["topic"].default: cls_
    for cls_ in get_concrete_subclasses(SubscriptionFilterBase)
    if cls_ is not UnknownSubscriptionFilter
}


def _handle_unknown_filter(value: Any) -> Any:
    if isinstance(value, dict):
        topic = value.get("topic")
        if topic not in _KNOWN_FILTERS:
            return UnknownSubscriptionFilter(**value)
        else:
            return _KNOWN_FILTERS[topic].model_validate(value)
    return value


SubscriptionFilter = Annotated[
    IntegrationsSubscriptionFilter
    | WorkflowsSubscriptionFilter
    | HostedExtractorsSubscriptionFilter
    | UnknownSubscriptionFilter,
    BeforeValidator(_handle_unknown_filter),
]


class SignalSubscription(BaseModelObject):
    external_id: str
    sink: SinkRef
    filter: SubscriptionFilter

    def as_id(self) -> SignalSubscriptionId:
        return SignalSubscriptionId(external_id=self.external_id)


class SignalSubscriptionRequest(SignalSubscription, UpdatableRequestResource):
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset()

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = self.as_id().dump(camel_case=True)
        filter_value = self.filter.dump(camel_case=True)
        update_item["update"] = {"filter": {"set": filter_value}}
        return update_item


class SignalSubscriptionResponse(SignalSubscription, ResponseResource[SignalSubscriptionRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> builtins.type[SignalSubscriptionRequest]:
        return SignalSubscriptionRequest
