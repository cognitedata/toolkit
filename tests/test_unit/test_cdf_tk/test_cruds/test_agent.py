from collections.abc import Hashable

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import DataModelIO, FunctionIO, ResourceIO, SkillIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.agent import AgentIO


class TestAgentIODumpResource:
    def test_dump_resource_ignores_empty_skills_when_omitted_locally(self) -> None:
        client = ToolkitClientMock()
        io = AgentIO(client, None, None)
        local = {
            "externalId": "my_agent",
            "name": "My Agent",
            "runtimeVersion": "0.9.9",
        }
        resource = AgentResponse.model_validate(
            {
                "externalId": "my_agent",
                "name": "My Agent",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "0.9.9",
                "skills": [],
            }
        )

        dumped = io.dump_resource(resource, local)

        assert dumped == local


class TestAgentIODependencies:
    def test_datamodel_is_in_class_dependencies(self) -> None:
        assert DataModelIO in AgentIO.dependencies

    def test_function_is_in_class_dependencies(self) -> None:
        assert FunctionIO in AgentIO.dependencies

    def test_skill_is_in_class_dependencies(self) -> None:
        if FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
            assert SkillIO in AgentIO.dependencies
        else:
            assert SkillIO not in AgentIO.dependencies

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
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "query",
                            "name": "Query",
                            "description": "Run flexible queries against your data model and scope.",
                            "configuration": {
                                "dataModels": {
                                    "type": "manual",
                                    "dataModels": [
                                        {
                                            "space": "my_space",
                                            "externalId": "my_data_model",
                                            "version": "v1",
                                            "viewExternalIds": ["MyView"],
                                        }
                                    ],
                                },
                                "instanceSpaces": {"type": "all"},
                            },
                        }
                    ],
                },
                [(DataModelIO, DataModelId(space="my_space", external_id="my_data_model", version="v1"))],
                id="query tool with manual data models yields DataModelIO dependency",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "query",
                            "name": "Query_Default",
                            "description": "Run flexible queries against your data model and scope.",
                            "configuration": {
                                "dataModels": {"type": "providedAtRuntime"},
                                "instanceSpaces": {"type": "providedAtRuntime"},
                            },
                        }
                    ],
                },
                [],
                id="query tool with providedAtRuntime yields no dependencies",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "tools": [
                        {
                            "type": "query",
                            "name": "Query_manual_scope",
                            "description": "Run flexible queries against your data model and scope.",
                            "configuration": {
                                "dataModels": {"type": "providedAtRuntime"},
                                "instanceSpaces": {"type": "manual", "spaces": ["akerbp_wi"]},
                            },
                        }
                    ],
                },
                [],
                id="query tool with manual instance spaces yields no dependencies",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "skills": ["my_skill", "other_skill"],
                },
                [
                    (SkillIO, ExternalId(external_id="my_skill")),
                    (SkillIO, ExternalId(external_id="other_skill")),
                ],
                id="skills yield SkillIO dependencies",
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
        if "skills" in item and not FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
            expected = []
        actual = list(AgentIO.get_dependent_items(item))

        assert actual == expected
