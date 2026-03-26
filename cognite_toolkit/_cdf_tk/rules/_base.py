from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import ClassVar

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module, SuccessfulReadYAMLFile
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource


class ToolkitRule(ABC):
    """A base rule class that defines the structure for all validation rules in the toolkit.
    It is defined in a such way to enable two different types of subclasses:
    - Rules that validates
    - and rules that validate across resources in a module (e.g. ModuleRule).

    """

    code: ClassVar[str]
    alpha: ClassVar[bool] = False
    fixable: ClassVar[bool] = False

    @abstractmethod
    def validate(self) -> Iterable[Insight]:
        raise NotImplementedError()


class ToolkitLocalRule(ToolkitRule):
    """Rule validating a module

    Args:
        module: The module to validate.
    """

    def __init__(self, module: Module) -> None:
        self.module = module

    def _get_validated_resources_with_file(self) -> Iterable[tuple[ToolkitResource, SuccessfulReadYAMLFile]]:
        for file in self.module.files:
            if not isinstance(file, SuccessfulReadYAMLFile):
                continue
            for resource in file.resources:
                if resource.validated is not None:
                    yield resource.validated, file


class ToolkitGlobalRule(ToolkitRule):
    """Rule validating the modules as a whole.

    Args:
        module: The module to validate.
        client: The ToolkitClient to use. This is required by some rules.

    """

    REQUIRES_CLIENT: ClassVar[bool] = False

    def __init__(self, modules: list[BuiltModule], client: ToolkitClient | None = None) -> None:
        self.modules = modules
        self.client = client
