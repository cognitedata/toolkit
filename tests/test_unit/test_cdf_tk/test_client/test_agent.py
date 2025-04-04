from cognite_toolkit._cdf_tk.client.data_classes.agents import Agent, AgentTool
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestAgentDataClass:
    def test_agent_data_class_dump_load(self):
        expected = FakeCogniteResourceGenerator(seed=1337).create_instance(Agent)
        dumped = expected.dump()
        actual = Agent._load(dumped)
        assert actual == expected

    def test_agent_tool_data_class_dump_load(self):
        agent_tool = FakeCogniteResourceGenerator(seed=1333).create_instance(AgentTool)
        dumped = agent_tool.dump()
        assert agent_tool == AgentTool._load(dumped)
