from typing import Any, Protocol, Self, TypeVar, runtime_checkable


@runtime_checkable
class ResourceRequestProtocol(Protocol):
    @classmethod
    def _load(cls, data: dict[str, Any], /) -> Self: ...

    def dump(self, camel_case: bool = True) -> dict[str, Any]: ...


@runtime_checkable
class ResourceResponseProtocol(Protocol):
    def as_write(self) -> ResourceRequestProtocol: ...


T_ResourceRequest = TypeVar("T_ResourceRequest", bound=ResourceRequestProtocol)
T_ResourceResponse = TypeVar("T_ResourceResponse", bound=ResourceResponseProtocol)
