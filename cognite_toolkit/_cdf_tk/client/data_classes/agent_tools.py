from __future__ import annotations

from abc import ABC
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


class AgentToolCore(WriteableCogniteResource["AgentToolWrite"], ABC):
    """
    Representation of an AI Agent Tool in CDF.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str) : The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the tool.
        description (str): The description of the tool.
        configuration (dict(str, Any)): The configuration of the tool.
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        description: str,
        configuration: dict[str, Any],
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.name = name
        self.description = description
        self.configuration = configuration

    def as_write(self) -> AgentToolWrite:
        return AgentToolWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            configuration=self.configuration,
        )


class AgentTool(AgentToolCore):
    """
    Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str) : The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the tool.
        description (str): The description of the tool.
        configuration (dict(str, Any)): The configuration of the tool.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentTool:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource["description"],
            configuration=resource["configuration"],
        )


class AgentToolWrite(AgentToolCore):
    """Representation of an AI Agent Tool in CDF.
    This is the read format of an agent tool.

    Args:
        external_id (str) : The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the tool.
        description (str): The description of the tool.
        configuration (dict(str, Any)): The configuration of the tool.
    """

    ...


class AgentToolWriteList(CogniteResourceList[AgentToolWrite]):
    _RESOURCE = AgentToolWrite


class AgentToolList(
    WriteableCogniteResourceList[AgentToolWrite, AgentTool],
    InternalIdTransformerMixin,
):
    _RESOURCE = AgentTool

    def as_write(self) -> AgentToolWriteList:
        """Returns this TransformationNotificationList instance"""
        return AgentToolWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())
