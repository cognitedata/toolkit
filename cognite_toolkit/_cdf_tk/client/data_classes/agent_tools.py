from abc import ABC

from cognite.client.data_classes._base import (
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


class AgentToolCore(WriteableCogniteResource["AgentToolWrite"], ABC):
    pass


class AgentTool(AgentToolCore):
    pass


class AgentToolWrite(AgentToolCore):
    pass


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
