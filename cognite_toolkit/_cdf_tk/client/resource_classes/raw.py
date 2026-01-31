import sys
from typing import Any

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import NameId, RawTableId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RAWDatabaseRequest(RequestResource):
    name: str = Field(alias="dbName")

    def as_id(self) -> NameId:
        return NameId(name=self.name)

    # Override dump to always use by_alias=False since the API expects name='...'
    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.

        """
        if exclude_extra:
            return self.model_dump(
                mode="json",
                by_alias=False,
                exclude_unset=True,
                exclude=set(self.__pydantic_extra__) if self.__pydantic_extra__ else None,
            )
        return self.model_dump(mode="json", by_alias=False, exclude_unset=True)


class RAWDatabaseResponse(ResponseResource[RAWDatabaseRequest]):
    name: str
    created_time: int

    def as_request_resource(self) -> RAWDatabaseRequest:
        return RAWDatabaseRequest.model_validate(self.dump(), extra="ignore")


class RAWTableRequest(RequestResource):
    # This is a query parameter, so we exclude it from serialization.
    db_name: str = Field(exclude=True)
    name: str = Field(alias="tableName")

    def as_id(self) -> RawTableId:
        return RawTableId(db_name=self.db_name, name=self.name)

    # Override dump to always use by_alias=False since the API expects name='...'
    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.

        """
        if exclude_extra:
            return self.model_dump(
                mode="json",
                by_alias=False,
                exclude_unset=True,
                exclude=set(self.__pydantic_extra__) if self.__pydantic_extra__ else None,
            )
        return self.model_dump(mode="json", by_alias=False, exclude_unset=True)

class RAWTableResponse(RequestResource, Identifier, ResponseResource["RAWTable"]):
    # This is a query parameter, so we exclude it from serialization.
    # Default to empty string to allow parsing from API responses (which don't include db_name).
    db_name: str = Field(default="", exclude=True)
    name: str

    def as_request_resource(self) -> "RAWTableResponse":
        dumped = {**self.dump(), "dbName": self.db_name}
        return type(self).model_validate(dumped, extra="ignore")

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource, by_name=True)
