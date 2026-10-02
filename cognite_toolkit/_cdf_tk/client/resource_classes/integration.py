"""Resource classes for the integrations API.

Based on https://api-docs.cognite.com/20230101-alpha/tag/Integrations
"""

from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, IntegrationConfigId

ActiveConfigRevision = int | Literal["local"]
IntegrationTaskType = Literal["continuous", "batch"]
IntegrationErrorLevel = Literal["warning", "error", "fatal"]
IntegrationErrorKind = Literal["general", "config", "task_never_closed", "seen_deadline_missed"]
IntegrationActionStatus = Literal["pending", "running", "failed", "succeeded", "cancel_pending", "canceled"]
IntegrationActionUpdateStatus = Literal["running", "failed", "succeeded", "canceled"]
IntegrationActionType = Literal["start_task", "stop_task", "custom"]
IntegrationTaskEventType = Literal["started", "ended"]


class IntegrationExtractor(BaseModelObject):
    """Extractor that an integration runs."""

    external_id: str
    version: str | None = None


class IntegrationTask(BaseModelObject):
    """A named unit of work reported by an extractor."""

    type: IntegrationTaskType | str
    name: str
    action: bool = False
    description: str | None = None
    sources: list[str] | None = None
    targets: list[str] | None = None


class AvailableIntegrationAction(BaseModelObject):
    """An action the extractor supports, reported at startup."""

    name: str
    type: IntegrationActionType | str
    description: str | None = None
    task: str | None = None


class Integration(BaseModelObject):
    external_id: str
    extractor: IntegrationExtractor
    name: str | None = None
    description: str | None = None
    documentation: str | None = None
    metadata: Metadata | None = None
    allowed_not_seen_minutes: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class IntegrationRequest(Integration, UpdatableRequestResource):
    """Request resource for creating and updating integrations.

    ``extractor`` is set on create and is not part of the update payload.
    """

    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = super().as_update(mode)
        update = update_item.get("update")
        if isinstance(update, dict):
            update.pop("extractor", None)
        return update_item


class IntegrationResponse(Integration, ResponseResource[IntegrationRequest]):
    created_time: int
    last_updated_time: int
    last_seen: int | None = None
    last_config_revision: int | None = None
    active_config_revision: ActiveConfigRevision | None = None
    tasks: list[IntegrationTask] | None = None

    @classmethod
    def request_cls(cls) -> type[IntegrationRequest]:
        return IntegrationRequest


class IntegrationTaskHistory(BaseModelObject):
    """One recorded run of an integration task."""

    task_name: str
    error_count: int
    warning_count: int
    fatal_count: int
    start_time: int
    message: str | None = None
    active_config_revision: ActiveConfigRevision | None = None
    end_time: int | None = None
    sources: list[str] | None = None
    targets: list[str] | None = None


class IntegrationErrorResponse(BaseModelObject):
    """A historical error reported by an integration."""

    type: IntegrationErrorKind | str
    level: IntegrationErrorLevel | str
    description: str
    details: str
    start_time: int
    task: str | None = None
    active_config_revision: ActiveConfigRevision | None = None
    end_time: int | None = None


class IntegrationCheckinErrorBase(BaseModelObject):
    """Fields shared by errors reported on check-in."""

    level: IntegrationErrorLevel | str
    description: str
    start_time: int
    details: str | None = None
    task: str | None = None
    end_time: int | None = None
    active_config_revision: ActiveConfigRevision | None = None


class GeneralIntegrationError(IntegrationCheckinErrorBase):
    """A check-in error that is not tied to a configuration revision."""

    type: Literal["general"] = "general"


class ConfigIntegrationError(IntegrationCheckinErrorBase):
    """A check-in error tied to a configuration revision."""

    type: Literal["config"]
    config_revision: int | None = None


IntegrationCheckinError = Annotated[GeneralIntegrationError | ConfigIntegrationError, Field(discriminator="type")]


class IntegrationTaskEvent(BaseModelObject):
    """A task start or stop reported on check-in."""

    type: IntegrationTaskEventType
    name: str
    timestamp: int
    message: str | None = None


class IntegrationActionUpdate(BaseModelObject):
    """Status update for an action, reported by the extractor on check-in."""

    external_id: str
    status: IntegrationActionUpdateStatus
    result_message: str | None = None
    result_metadata: Metadata | None = None


class IntegrationStartupRequest(RequestResource):
    """Report extractor info and mark the integration as started."""

    external_id: str
    extractor: IntegrationExtractor
    tasks: list[IntegrationTask] | None = None
    active_config_revision: ActiveConfigRevision | None = None
    timestamp: int | None = None
    available_actions: list[AvailableIntegrationAction] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class IntegrationActionRequest(RequestResource):
    external_id: str
    action_name: str
    call_metadata: Metadata | None = None
    # Query parameter on create, not part of the action body.
    integration_external_id: str = Field(exclude=True)

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class IntegrationActionResponse(ResponseResource[IntegrationActionRequest]):
    external_id: str
    action_name: str
    status: IntegrationActionStatus | str
    created_time: int
    last_updated_time: int
    call_metadata: Metadata | None = None
    result_message: str | None = None
    result_metadata: Metadata | None = None
    # Not returned by the API. Set from the request or list filter when known.
    integration_external_id: str = Field("", exclude=True)

    @classmethod
    def request_cls(cls) -> type[IntegrationActionRequest]:
        return IntegrationActionRequest

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_request_resource(self) -> IntegrationActionRequest:
        dumped = self.dump()
        dumped["integrationExternalId"] = self.integration_external_id
        return IntegrationActionRequest.model_validate(dumped, extra="ignore")


class IntegrationCheckinResponse(BaseModelObject):
    """Response shared by startup and check-in."""

    external_id: str
    last_config_revision: int | None = None
    pending_actions: list[IntegrationActionResponse] | None = None


class IntegrationCheckinRequest(RequestResource):
    """Periodic heartbeat with task events, errors, and action updates."""

    external_id: str
    task_events: list[IntegrationTaskEvent] | None = None
    errors: list[IntegrationCheckinError] | None = None
    action_updates: list[IntegrationActionUpdate] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class IntegrationSyncResponse(BaseModelObject):
    """Incremental task history and errors since the previous cursor."""

    next_cursor: str
    more_data: bool
    history: list[IntegrationTaskHistory] | None = None
    errors: list[IntegrationErrorResponse] | None = None


class IntegrationConfig(BaseModelObject):
    external_id: str
    config: str
    description: str | None = None

    def as_id(self) -> IntegrationConfigId:
        return IntegrationConfigId(external_id=self.external_id)


class IntegrationConfigRequest(IntegrationConfig, RequestResource):
    """Request body for creating a configuration revision."""

    ...


class IntegrationConfigResponse(IntegrationConfig, ResponseResource[IntegrationConfigRequest]):
    """A configuration revision returned by create and retrieve, including the config body."""

    revision: int
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[IntegrationConfigRequest]:
        return IntegrationConfigRequest

    def as_id(self) -> IntegrationConfigId:
        return IntegrationConfigId(external_id=self.external_id, revision=self.revision)


class IntegrationConfigListResponse(BaseModelObject):
    """Configuration revision metadata returned by list.

    The config body is omitted.
    """

    external_id: str
    description: str | None = None
    revision: int
    created_time: int
    last_updated_time: int | None = None  # Bug in API. This is not returned.

    def as_id(self) -> IntegrationConfigId:
        return IntegrationConfigId(external_id=self.external_id, revision=self.revision)
