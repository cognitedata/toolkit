from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Recommendation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource

T_Resources = TypeVar("T_Resources", bound=list[ToolkitResource] | Module)


class ToolkitRule(ABC, Generic[T_Resources]):
    """A base rule class that defines the structure for all validation rules in the toolkit.
    It is defined in a such way to enable two different types of subclasses:
    - rules that validate individual resources (e.g. DataModelRule)
    - and rules that validate across resources in a module (e.g. ModuleRule).

    """

    code: ClassVar[str]
    resource_type: ClassVar[str]
    insight_type: ClassVar[type[ConsistencyError] | type[Recommendation]]
    alpha: ClassVar[bool] = False
    fixable: ClassVar[bool] = False

    def __init__(
        self,
        resources: T_Resources,
    ) -> None:
        self.resources = resources

    @abstractmethod
    def validate(self) -> list[ConsistencyError] | list[Recommendation] | list[ConsistencyError | Recommendation]:
        """Execute rule validation."""
        ...


class ToolkitResourceRule(ToolkitRule[list[ToolkitResource]]):
    """Rule for toolkit resource validation principles."""

    pass


class ToolkitModuleRule(ToolkitRule[Module]):
    """Rule for toolkit module validation principles."""

    pass
