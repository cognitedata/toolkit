from __future__ import annotations

from collections.abc import Sequence

from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._api.data_classes import Module, ModuleList


class ModulesAPI:
    def __init__(self) -> None:
        raise NotImplementedError

    def list(
        self,
    ) -> ModuleList:
        raise NotImplementedError

    def retrieve(self, module: str | SequenceNotStr[str]) -> Module:
        raise NotImplementedError

    def deploy(self, module: Module | Sequence[Module], include: set, exclude: set) -> Module | ModuleList:
        raise NotImplementedError

    def clean(self, module: Module | Sequence[Module], include: set, exclude: set) -> Module | ModuleList:
        raise NotImplementedError
