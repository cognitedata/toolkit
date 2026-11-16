from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
)
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import contains_unresolved_variable
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
)
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO, ResourceType

from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)
from ..client import ToolkitClient


class Failing(ToolkitRuleSet):
    """Checks for failing resources, i.e. workflows, transformations, functions, and hosted extractors that
    are currently failing in CDF.
    """

    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        inventory = self._load_inventory(self.client)
        standardized = self._standardize(inventory)
        failures_with_reason = self._identify_reason(standardized)
        yield from self._reprot_failures(failures_with_reason)


    def _load_inventory(self, client: ToolkitClient) -> None:
        """Loads the inventory of resources from CDF and yields insights for any failing resources."""
        ...

    def _standardize(self) -> None:
        """Ensure that resources that are part of a failing workflow, is not reported twice, and that it is in the workflow failure instead"""
        ...

