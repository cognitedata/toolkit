from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from cognite_toolkit._cdf_tk.data_classes._insights import (
    Insight,
)
from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
)
from ..client import ToolkitClient
from ..commands.deploy_v2.command import DeploymentResult


class Failing(ToolkitRuleSet):
    """Checks for failing resources, i.e. workflows, transformations, functions, and hosted extractors that
    are currently failing in CDF.
    """
    def __init__(
        self,
        from_timestamp: datetime | None = None,
        resources: Sequence[ResourceRef] | None = None,
        results: Sequence[DeploymentResult] | None = None,
        client: ToolkitClient | None = None,
    ) -> None:
        super().__init__(resources=resources, results=results, client=client)
        self.from_timestamp = from_timestamp

    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        inventory = self._load_inventory(self.client)
        standardized = self._standardize(inventory)
        failures_with_reason = self._identify_reason(standardized)
        yield from self._report_failures(failures_with_reason)

    def _load_inventory(self, client: ToolkitClient) -> Sequence[Any]:
        """Loads the inventory of resources from CDF and yields insights for any failing resources."""
        client.workflows.versions.list()
        return []

    def _standardize(self, inventory: Sequence[Any]) -> Sequence[Any]:
        """Ensure that resources that are part of a failing workflow, is not reported twice, and that it is in the workflow failure instead"""
        ...

    def _identify_reason(self, failures: Sequence[Any]) -> Sequence[Any]:
        """Identify the reason for the failure, e.g. if it is a workflow, transformation, function, or hosted extractor"""
        ...

    def _report_failures(self, failures: Any) -> Iterable[Insight | InternalValidatorException]:
        """Report the failures as insights"""
        ...
