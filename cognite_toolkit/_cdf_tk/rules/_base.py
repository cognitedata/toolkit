from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import ClassVar, Literal

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import FailedValidation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module, SuccessfulReadYAMLFile
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource


class ToolkitLocalRule(ABC):
    """Rule validating a module

    Args:
        module: The module to validate.
    """

    CODE: ClassVar[str]
    IS_ALPHA: ClassVar[bool] = False
    IS_FIXABLE: ClassVar[bool] = False

    def __init__(self, module: Module) -> None:
        self.module = module

    @abstractmethod
    def validate(self) -> Iterable[Insight]:
        raise NotImplementedError()

    def _get_validated_resources_with_file(self) -> Iterable[tuple[ToolkitResource, SuccessfulReadYAMLFile]]:
        for file in self.module.files:
            if not isinstance(file, SuccessfulReadYAMLFile):
                continue
            for resource in file.resources:
                if resource.validated is not None:
                    yield resource.validated, file


class RuleSetStatus(BaseModel):
    code: Literal["ready", "reduced", "skip", "unavailable"]
    message: str | None = None


class ToolkitGlobalRuleSet(ABC):
    """Validation of all modules as a whole.

    This can output different

    Args:
        module: The module to validate.
        client: The ToolkitClient to use. This is required by some rules.
    """

    CODE_PREFIX: ClassVar[str]
    DISPLAY_NAME: ClassVar[str]

    def __init__(self, modules: list[BuiltModule], client: ToolkitClient | None = None) -> None:
        self.modules = modules
        self.client = client

    @abstractmethod
    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    @abstractmethod
    def validate(self) -> Iterable[Insight | FailedValidation]:
        raise NotImplementedError()
