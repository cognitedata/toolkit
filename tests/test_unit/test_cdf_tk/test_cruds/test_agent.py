from collections.abc import Hashable

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ExternalId
from cognite_toolkit._cdf_tk.resource_ios import DataModelIO, FunctionIO, ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.agent import AgentIO


class TestAgentIODependencies:
    def test_datamodel_is_in_class_dependencies(self) -> None:
        assert DataModelIO in AgentIO.dependencies

    def test_function_is_in_class_dependencies(self) -> None:
        assert FunctionIO in AgentIO.dependencies

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "callFunction",
                            "name": "call_fn",
                            "description": "Calls a function",
                            "configuration": {"externalId": "my_function"},
                        }
                    ],
                },
                [(FunctionIO, ExternalId(external_id="my_function"))],
                id="callFunction tool yields FunctionIO dependency",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "queryKnowledgeGraph",
                            "name": "query_kg",
                            "description": "Queries the knowledge graph",
                            "configuration": {
                                "dataModels": [
                                    {
                                        "space": "my_space",
                                        "externalId": "my_data_model",
                                        "version": "v1",
                                        "viewExternalIds": ["MyView"],
                                    }
                                ],
                                "instanceSpaces": {"type": "all"},
                            },
                        }
                    ],
                },
                [
                    (
                        DataModelIO,
                        DataModelId(space="my_space", external_id="my_data_model", version="v1"),
                    )
                ],
                id="queryKnowledgeGraph tool yields DataModelIO dependency",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "queryKnowledgeGraph",
                            "name": "query_kg",
                            "description": "Queries the knowledge graph",
                            "configuration": {
                                "dataModels": [
                                    {
                                        "space": "space_a",
                                        "externalId": "model_a",
                                        "version": "1",
                                        "viewExternalIds": ["ViewA"],
                                    },
                                    {
                                        "space": "space_b",
                                        "externalId": "model_b",
                                        "version": "2",
                                        "viewExternalIds": ["ViewB"],
                                    },
                                ],
                                "instanceSpaces": {"type": "all"},
                            },
                        }
                    ],
                },
                [
                    (DataModelIO, DataModelId(space="space_a", external_id="model_a", version="1")),
                    (DataModelIO, DataModelId(space="space_b", external_id="model_b", version="2")),
                ],
                id="queryKnowledgeGraph with multiple data models yields multiple DataModelIO dependencies",
            ),
            pytest.param(
                {"externalId": "my_agent", "tools": []},
                [],
                id="agent with no tools yields no dependencies",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [{"type": "analyzeImage", "name": "analyze_img", "description": "Analyzes an image"}],
                },
                [],
                id="tool with no dependency yields nothing",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceIO], Hashable]]) -> None:
        actual = list(AgentIO.get_dependent_items(item))

        assert actual == expected
