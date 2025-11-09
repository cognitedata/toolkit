import sys
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    pass

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ResourceRequestProtocol(Protocol):
    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self: ...

    def dump(self, camel_case: bool = True) -> dict[str, Any]: ...


class ResourceResponseProtocol(Protocol):
    def as_write(self) -> ResourceRequestProtocol: ...


T_ResourceRequest = TypeVar("T_ResourceRequest", bound=ResourceRequestProtocol)
T_ResourceResponse = TypeVar("T_ResourceResponse", bound=ResourceResponseProtocol)


class ResourceRequestListProtocol(Protocol, Generic[T_ResourceRequest]):
    @classmethod
    def load(cls, data: list[dict[str, Any]]) -> Self: ...


class ResourceResponseListProtocol(Protocol, Generic[T_ResourceResponse]):
    def as_write(self) -> ResourceRequestListProtocol: ...

    @classmethod
    def load(cls, data: list[dict[str, Any]]) -> Self: ...


T_ResourceRequestList = TypeVar("T_ResourceRequestList", bound=ResourceRequestListProtocol)
T_ResourceResponseList = TypeVar("T_ResourceResponseList", bound=ResourceResponseListProtocol)
