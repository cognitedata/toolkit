from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cognite.client.utils._identifier import Identifier

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.data_classes._insights import Insight, InternalValidatorException
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionIO,
    HostedExtractorJobIO,
    ResourceType,
    SpaceIO,
    TransformationIO,
    WorkflowIO,
)
from cognite_toolkit._cdf_tk.rules import ValidationInput
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms


@dataclass
class _UnusedResourceTypes:
    type: ResourceType
    identifiers: Sequence[Identifier]
    message: str


class UnusedRuleSet(ToolkitRuleSet):
    DISPLAY_NAME = "Unused resources"
    RESOURCE_TYPES = frozenset(
        {
            WorkflowIO.as_resource_type(),
            FunctionIO.as_resource_type(),
            HostedExtractorJobIO.as_resource_type(),
            TransformationIO.as_resource_type(),
            SpaceIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})

    def __init__(self, client: ToolkitClient | None = None, created_before: timedelta = timedelta(days=7)) -> None:
        super().__init__(client=client)
        self.created_before = created_before

    def get_status(self, data: ValidationInput) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="A client is required to check for unused resources in CDF.")
        else:
            return RuleSetStatus(code="ready", message="Ready to check for unused resources in CDF.")

    def validate(self, data: ValidationInput) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        created_before_epoch = datetime_to_ms(datetime.now(UTC) - self.created_before)
        unused_resources = self._load_unused(self.client, created_before_epoch)
        yield from self._report_unused(unused_resources)

    def _load_unused(self, client: ToolkitClient, created_before_epoch: int) -> list[_UnusedResourceTypes]:
        unused_resources: list[_UnusedResourceTypes] = []
        unused_resources.append(self._unused_spaces(client))

        return unused_resources

    def _report_unused(self, unused_resources: list[_UnusedResourceTypes]) -> Iterable[Insight]:
        raise NotImplementedError()

    @classmethod
    def _unused_spaces(cls, client: ToolkitClient) -> _UnusedResourceTypes:
        results = client.statistics.spaces.list()
        unused_spaces = [space.space for space in results if space.is_empty]
        return _UnusedResourceTypes(
            type=SpaceIO.as_resource_type(),
            identifiers=[SpaceId(space=space) for space in unused_spaces],
            message="There are no instances, views, containers, or other resources in this space",
        )
