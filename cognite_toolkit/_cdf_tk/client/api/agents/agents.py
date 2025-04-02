from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.agents import Agent, AgentList, AgentWrite


class AgentsAPI(APIClient):
    _RESOURCE_PATH = "/ai/agents"

    @overload
    def __call__(self) -> Iterator[Agent]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[AgentList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Agent] | Iterator[AgentList]:
        """Iterate over agents.

        Args:
            chunk_size: The number of agents to return in each chunk. None will return all agents.

        Yields:
            Agent or AgentList

        """
        return self._list_generator(method="GET", resource_cls=Agent, list_cls=AgentList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Agent]:
        return self.__call__()

    @overload
    def create(self, agent: AgentWrite) -> Agent: ...

    @overload
    def create(self, agent: Sequence[AgentWrite]) -> AgentList: ...

    def create(self, agent: AgentWrite | Sequence[AgentWrite]) -> Agent | AgentList:
        """Create a new agent.

        Args:
            agent: AgentWrite or list of AgentWrite.

        Returns:
            Agent object.

        """
        headers = {"cdf-version": "alpha"}
        return self._create_multiple(
            list_cls=AgentList,
            resource_cls=Agent,
            items=agent,
            input_resource_cls=AgentWrite,
            headers=headers,
        )

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> AgentList:
        """Retrieve an agent.

        Args:
            data_set_id: Data set id of the agent.

        Returns:
            Agent object.

        """
        body = self._create_body(external_id, True)

        headers = {"cdf-version": "alpha"}
        self._cognite_client.config.debug = True

        res = self._post(url_path=self._RESOURCE_PATH + "/byids", json=body, headers=headers)
        return AgentList._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _create_body(external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> dict:
        ids = [external_id] if isinstance(external_id, str) else external_id
        body = {"items": [{"externalId": external_id} for external_id in ids], "ignoreUnknownIds": ignore_unknown_ids}

        return body

    @overload
    def update(self, agent: AgentWrite) -> Agent: ...

    @overload
    def update(self, agent: Sequence[AgentWrite]) -> AgentList: ...

    def update(self, agent: AgentWrite | Sequence[AgentWrite]) -> Agent | AgentList:
        """Update an agent.

        Args:
            agent: AgentWrite or list of AgentWrite.

        Returns:
            Agent or AgentList object.

        """
        is_single = False
        if isinstance(agent, AgentWrite):
            agents = [agent]
            is_single = True
        elif isinstance(agent, Sequence):
            agents = list(agent)
        else:
            raise ValueError("agent must be a AgentWrite or a list of AgentWrite")

        # THE agent API does not support update
        res = self._post(url_path=self._RESOURCE_PATH + "/update", json={"items": agents})
        loaded = AgentList._load(res.json()["items"], cognite_client=self._cognite_client)
        return loaded[0] if is_single else loaded

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete an agent.

        Args:
            external_id: External ID of the agent.

        Returns:
            None

        """
        body = self._create_body(external_id)
        self._post(url_path=self._RESOURCE_PATH + "/delete", json=body)

    def list(self) -> AgentList:
        """List agents.

        Returns:
            AgentList

        """
        return self._list(method="GET", resource_cls=Agent, list_cls=AgentList)
