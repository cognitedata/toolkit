import sys
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

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


class ResourceRequestListProtocol(Protocol, Sized): ...


class ResourceResponseListProtocol(Protocol, Sized): ...


T_ResourceRequest = TypeVar("T_ResourceRequest", bound=ResourceRequestProtocol)
T_ResourceResponse = TypeVar("T_ResourceResponse", bound=ResourceResponseProtocol)
T_ResourceRequestList = TypeVar("T_ResourceRequestList", bound=ResourceRequestListProtocol)
T_ResourceResponseList = TypeVar("T_ResourceResponseList", bound=ResourceResponseListProtocol)
