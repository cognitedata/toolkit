from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


@dataclass
class AgentToolCore(WriteableCogniteResource["AgentToolWrite"], ABC):
    """
    Representation of an AI Agent Tool in CDF.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the tool.
        description (str | None): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    external_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[dict[str, Any]] = None

    def as_write(self) -> AgentToolWrite:
        return AgentToolWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            configuration=self.configuration,
        )


@dataclass
class AgentTool(AgentToolCore):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the tool.
        description (str | None): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    id: Optional[int] = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: Optional[CogniteClient] = None) -> AgentTool:
        return cls(
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            name=resource.get("name"),
            description=resource.get("description"),
            configuration=resource.get("configuration"),
        )


@dataclass
class AgentToolWrite(AgentToolCore):
    """Representation of an AI Agent Tool in CDF.
    This is the write format of an agent tool.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the tool.
        description (str | None): The description of the tool.
        configuration (dict[str, Any] | None): The configuration of the tool.
    """

    pass


class AgentToolWriteList(CogniteResourceList[AgentToolWrite]):
    _RESOURCE = AgentToolWrite


class AgentToolList(
    WriteableCogniteResourceList[AgentToolWrite, AgentTool],
    InternalIdTransformerMixin,
):
    _RESOURCE = AgentTool

    def as_write(self) -> AgentToolWriteList:
        """Returns this AgentToolWriteList instance"""
        return AgentToolWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
