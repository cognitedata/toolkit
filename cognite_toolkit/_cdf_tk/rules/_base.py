from abc import ABC, abstractmethod
from typing import ClassVar

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Recommendation
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class ToolkitRule(ABC):
    """Rule for toolkit resource validation principles."""

    code: ClassVar[str]
    resource_type: ClassVar[str]
    insight_type: ClassVar[type[ConsistencyError] | type[Recommendation]]
    alpha: ClassVar[bool] = False
    fixable: ClassVar[bool] = False

    def __init__(
        self,
        resources: list[ToolkitResource],
    ) -> None:
        self.resources = resources

    @abstractmethod
    def validate(self) -> list[ConsistencyError] | list[Recommendation] | list[ConsistencyError | Recommendation]:
        """Execute rule validation."""
        ...
