from __future__ import annotations

from abc import ABC

# We need to import Sequence from typing and not collections.abc for
# cognite_toolkit._parameters.read_parameter_from_init_type_hints to work on Python 3.9
# is necessary to avoid Ruff changing the import
from typing import Any, ClassVar, Literal, NoReturn, Sequence  # noqa

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from typing_extensions import Self


class _ApplicationEntityCore(WriteableCogniteResource["ApplicationEntityWrite"], ABC):
    def __init__(
        self,
        external_id: str,
        visibility: Literal["public", "private"] = "private",
        data: dict[str, Any] | None = None,
    ) -> None:
        self.external_id = external_id
        self.visibility = visibility
        self.data = data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            visibility=resource.get("visibility", "private"),
            data=resource.get("data"),
        )


class ApplicationEntityWrite(_ApplicationEntityCore):
    def as_write(self) -> ApplicationEntityWrite:
        return self


class ApplicationEntity(_ApplicationEntityCore):
    def __init__(
        self,
        external_id: str,
        visibility: Literal["public", "private"],
        data: dict[str, Any] | None,
        created_time: int | None,
        last_updated_time: int | None,
    ) -> None:
        super().__init__(external_id, visibility, data)
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            visibility=resource["visibility"],
            data=resource.get("data"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
        )

    def as_write(self) -> ApplicationEntityWrite:
        return ApplicationEntityWrite(
            external_id=self.external_id,
            visibility=self.visibility,
            data=self.data,
        )


class ApplicationEntityWriteList(CogniteResourceList[ApplicationEntityWrite]):
    _RESOURCE = ApplicationEntityWrite


class ApplicationEntityList(WriteableCogniteResourceList[ApplicationEntityWrite, ApplicationEntity]):
    _RESOURCE = ApplicationEntity

    def as_write(self) -> ApplicationEntityWriteList:
        return ApplicationEntityWriteList([x.as_write() for x in self.data])
