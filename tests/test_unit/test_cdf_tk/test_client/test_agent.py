from cognite_toolkit._cdf_tk.client.data_classes.agents import Agent
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestAgentDataClass:
    def test_agent_data_class_dump_load(self):
        agent = FakeCogniteResourceGenerator(seed=1337).create_instance(Agent)
        dumped = agent.dump()
        assert agent == Agent._load(dumped)
