from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class BaseModelObject(BaseModel, alias_generator=to_camel, extra="allow"):
    """Base class for all object. This includes resources and nested objects."""

    ...


class RequestResource(BaseModel): ...


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


class ResponseResource(BaseModel, Generic[T_RequestResource], ABC):
    @abstractmethod
    def as_request_resource(self) -> T_RequestResource:
        """Convert the response resource to a request resource."""
        ...
