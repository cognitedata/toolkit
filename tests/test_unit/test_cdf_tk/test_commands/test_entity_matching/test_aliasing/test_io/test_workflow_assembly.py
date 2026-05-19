import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.workflow_assembly import (
    WorkflowBundle,
    WorkflowVersionAssembly,
    WorkflowVersionAssemblyRequest,
)


@pytest.fixture
def assembly() -> WorkflowVersionAssembly:
    return WorkflowVersionAssembly()


@pytest.fixture
def simple_rule() -> AliasingRule:
    return AliasingRule(
        name="simple_rule",
        rule_type="character_substitution",
        description="Simple test rule",
        payload={"replacements": {"a": "b"}},
    )


@pytest.fixture
def default_key_path() -> str:
    return 'properties.cdf_cdm["CogniteAsset/v1"].name'


@pytest.fixture
def simple_kuiper() -> AliasingKuiper:
    return AliasingKuiper(expression='input.keys.map(k => ({...k, aliases: [k.external_id.replace("a", "b")]}))')


@pytest.fixture
def rule_with_special_chars() -> AliasingRule:
    return AliasingRule(
        name="rule_with_quotes",
        rule_type="regex_substitution",
        description='Rule with "special" characters',
        payload={"pattern": '"', "replacement": ""},
    )


@pytest.fixture
def kuiper_with_special_chars() -> AliasingKuiper:
    return AliasingKuiper(expression='input.keys.map(k => k.external_id.includes("test"))')


class TestWorkflowVersionAssembly:
    def test_when_build_called_with_defaults_then_returns_workflow_bundle(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert isinstance(result, WorkflowBundle)
        assert result.workflow_yaml is not None
        assert result.workflow_version_yaml is not None

    def test_when_build_called_then_includes_default_external_id(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "externalId: entity_matching_aliasing" in result.workflow_yaml

    def test_when_build_called_then_includes_default_workflow_description(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "description: Entity matching aliasing workflow" in result.workflow_yaml

    def test_when_build_called_with_custom_external_id_then_uses_custom_id(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        custom_id = "my_custom_workflow"
        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(simple_rule, simple_kuiper)],
                key_path=default_key_path,
                workflow_external_id=custom_id,
            )
        )

        assert f"externalId: {custom_id}" in result.workflow_yaml
        assert f"workflowExternalId: {custom_id}" in result.workflow_version_yaml

    def test_when_build_called_with_custom_description_then_uses_custom_description(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        custom_description = "My custom workflow description"
        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(simple_rule, simple_kuiper)],
                key_path=default_key_path,
                workflow_description=custom_description,
            )
        )

        assert f"description: {custom_description}" in result.workflow_yaml

    def test_when_build_called_with_single_rule_then_creates_single_task(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "aliasing_task_simple_rule" in result.workflow_version_yaml
        assert "jsonMapping" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_rule_name(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert f"name: {simple_rule.name}" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_rule_description(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert f"description: {simple_rule.description}" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_kuiper_expression(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        escaped_expression = simple_kuiper.expression.replace('"', '\\"')
        assert escaped_expression in result.workflow_version_yaml

    def test_when_build_called_with_expression_containing_quotes_then_escapes_quotes(
        self,
        assembly: WorkflowVersionAssembly,
        rule_with_special_chars: AliasingRule,
        kuiper_with_special_chars: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule_with_special_chars, kuiper_with_special_chars)], key_path=default_key_path
            )
        )

        escaped_expression = kuiper_with_special_chars.expression.replace('"', '\\"')
        assert escaped_expression in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_creates_multiple_tasks(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        rule1 = AliasingRule(
            name="rule_one",
            rule_type="character_substitution",
            description="First rule",
            payload={"replacements": {"a": "b"}},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule_two",
            rule_type="character_substitution",
            description="Second rule",
            payload={"replacements": {"c": "d"}},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "aliasing_task_rule_one" in result.workflow_version_yaml
        assert "aliasing_task_rule_two" in result.workflow_version_yaml
        assert "name: rule_one" in result.workflow_version_yaml
        assert "name: rule_two" in result.workflow_version_yaml

    def test_when_build_called_then_workflow_version_includes_required_fields(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "workflowExternalId: entity_matching_aliasing" in result.workflow_version_yaml
        assert "version: v1" in result.workflow_version_yaml
        assert "workflowDefinition:" in result.workflow_version_yaml
        assert "description: Entity matching aliasing workflow" in result.workflow_version_yaml
        assert "tasks:" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_required_parameters(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "type: jsonMapping" in result.workflow_version_yaml
        assert "retries: 0" in result.workflow_version_yaml
        assert "onFailure: abortWorkflow" in result.workflow_version_yaml
        assert "jsonMapping:" in result.workflow_version_yaml
        assert "inputs:" in result.workflow_version_yaml
        assert "- input" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_workflow_input_reference(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "${workflow.input}" in result.workflow_version_yaml

    def test_when_build_workflow_called_with_defaults_then_returns_valid_yaml(
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        result = assembly._build_workflow("entity_matching_aliasing", "Entity matching aliasing workflow")

        assert "externalId: entity_matching_aliasing" in result
        assert "description: Entity matching aliasing workflow" in result

    def test_when_build_workflow_called_with_custom_values_then_uses_custom_values(
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        custom_id = "custom_id"
        custom_desc = "custom_description"
        result = assembly._build_workflow(external_id=custom_id, description=custom_desc)

        assert f"externalId: {custom_id}" in result
        assert f"description: {custom_desc}" in result

    def test_when_build_workflow_version_called_with_single_rule_then_creates_single_task(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly._build_workflow_version([(simple_rule, simple_kuiper)], default_key_path)

        assert "aliasing_task_simple_rule" in result
        assert f"name: {simple_rule.name}" in result

    def test_when_build_workflow_version_called_with_multiple_rules_then_all_tasks_present(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        rule3 = AliasingRule(
            name="rule3",
            rule_type="character_substitution",
            description="Rule 3",
            payload={},
        )
        kuiper3 = AliasingKuiper(expression="expr3")

        result = assembly._build_workflow_version(
            [(rule1, kuiper1), (rule2, kuiper2), (rule3, kuiper3)], default_key_path
        )

        assert "aliasing_task_rule1" in result
        assert "aliasing_task_rule2" in result
        assert "aliasing_task_rule3" in result
        assert "name: rule1" in result
        assert "name: rule2" in result
        assert "name: rule3" in result

    def test_when_build_workflow_version_called_then_external_id_derived_from_workflow_id(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        custom_workflow_id = "my_workflow"
        result = assembly._build_workflow_version(
            [(simple_rule, simple_kuiper)], default_key_path, workflow_external_id=custom_workflow_id
        )

        assert f"workflowExternalId: {custom_workflow_id}" in result

    def test_when_expression_contains_quotes_then_properly_escaped_in_task(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule = AliasingRule(
            name="test_rule",
            rule_type="regex_substitution",
            description="Test rule with quotes",
            payload={},
        )
        kuiper = AliasingKuiper(expression='test.replace("old", "new")')

        result = assembly._build_workflow_version([(rule, kuiper)], default_key_path)

        escaped_expression = 'test.replace(\\"old\\", \\"new\\")'
        assert escaped_expression in result

    def test_when_expression_has_mixed_quotes_then_only_double_quotes_escaped(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="Test rule",
            payload={},
        )
        kuiper = AliasingKuiper(expression="test.replace('single', \"double\")")

        result = assembly._build_workflow_version([(rule, kuiper)], default_key_path)

        escaped_expression = "test.replace('single', \\\"double\\\")"
        assert escaped_expression in result

    def test_when_build_called_then_workflow_yaml_properly_formatted(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        lines = result.workflow_yaml.strip().split("\n")
        assert len(lines) >= 2
        assert lines[0].startswith("externalId:")
        assert lines[1].startswith("description:")

    def test_when_build_called_then_workflow_version_yaml_properly_indented(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        yaml_lines = result.workflow_version_yaml.split("\n")
        tasks_section_found = False
        for i, line in enumerate(yaml_lines):
            if "tasks:" in line:
                tasks_section_found = True
                if i + 1 < len(yaml_lines):
                    next_line = yaml_lines[i + 1]
                    assert next_line.startswith("    - externalId:"), "Task should be indented with 4 spaces"

        assert tasks_section_found, "tasks: section should be present"

    def test_when_build_called_with_rule_containing_underscores_then_task_id_reflects_name(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule = AliasingRule(
            name="rule_with_underscores_name",
            rule_type="character_substitution",
            description="Test",
            payload={},
        )
        kuiper = AliasingKuiper(expression="test_expr")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(rule, kuiper)], key_path=default_key_path)
        )

        assert "aliasing_task_rule_with_underscores_name" in result.workflow_version_yaml

    def test_when_build_called_with_single_rule_then_combiner_task_created(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "externalId: combiner_task" not in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_combiner_task_created(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "externalId: combiner_task" in result.workflow_version_yaml
        assert "type: jsonMapping" in result.workflow_version_yaml

    def test_when_build_called_then_combiner_task_has_correct_name(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "name: Combiner" in result.workflow_version_yaml

    def test_when_build_called_then_combiner_task_has_correct_description(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "description: Combines results from all aliasing tasks" in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_combiner_depends_on_aliasing_tasks(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "dependsOn:" in result.workflow_version_yaml
        assert "- externalId: aliasing_task_rule1" in result.workflow_version_yaml
        assert "- externalId: aliasing_task_rule2" in result.workflow_version_yaml

    def test_when_build_called_then_combiner_includes_kuiper_expression(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "#get_external_ids :=" in result.workflow_version_yaml
        assert "#get_aliases_for_group :=" in result.workflow_version_yaml
        assert "get_external_ids(input.aliasing_task_results)" in result.workflow_version_yaml

    def test_when_build_called_then_combiner_has_aliasing_task_results_input(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "aliasing_task_results" in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_combiner_input_includes_task_outputs(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule_one",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule_two",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "${aliasing_task_rule_one.output.result}" in result.workflow_version_yaml
        assert "${aliasing_task_rule_two.output.result}" in result.workflow_version_yaml

    def test_when_build_called_then_combiner_task_placed_after_aliasing_tasks(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        aliasing_task_pos = result.workflow_version_yaml.find("externalId: aliasing_task_rule1")
        combiner_task_pos = result.workflow_version_yaml.find("externalId: combiner_task")

        assert aliasing_task_pos != -1
        assert combiner_task_pos != -1
        assert aliasing_task_pos < combiner_task_pos

    def test_when_build_called_then_combiner_has_proper_parameters(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert "retries: 0" in result.workflow_version_yaml
        assert "onFailure: abortWorkflow" in result.workflow_version_yaml

    def test_when_build_called_with_expression_containing_quotes_then_combiner_expression_escaped(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        assert '\\"external_id\\"' in result.workflow_version_yaml
        assert '\\"space\\"' in result.workflow_version_yaml
        assert '\\"aliases\\"' in result.workflow_version_yaml

    def test_when_build_called_then_key_extraction_task_present(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "externalId: key_extraction_task" in result.workflow_version_yaml
        assert "name: Key Extraction" in result.workflow_version_yaml
        assert "description: Extracts keys from input nodes" in result.workflow_version_yaml

    def test_when_build_called_then_key_extraction_task_appears_first(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        key_extraction_pos = result.workflow_version_yaml.find("externalId: key_extraction_task")
        aliasing_task_pos = result.workflow_version_yaml.find("externalId: aliasing_task_simple_rule")

        assert key_extraction_pos != -1
        assert aliasing_task_pos != -1
        assert key_extraction_pos < aliasing_task_pos

    def test_when_build_called_then_aliasing_tasks_depend_on_key_extraction(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "dependsOn:" in result.workflow_version_yaml
        assert "- externalId: key_extraction_task" in result.workflow_version_yaml

    def test_when_build_called_then_key_extraction_task_has_correct_expression(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        assert "external_id" in result.workflow_version_yaml
        assert "properties.cdf_cdm" in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_aliasing_tasks_depend_on_key_extraction(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        yaml_lines = result.workflow_version_yaml.split("\n")
        found_dependency = False
        for i, line in enumerate(yaml_lines):
            if "externalId: aliasing_task_rule1" in line or "externalId: aliasing_task_rule2" in line:
                for j in range(i, min(i + 5, len(yaml_lines))):
                    if "dependsOn:" in yaml_lines[j]:
                        found_dependency = True
                        break
        assert found_dependency

    def test_when_build_called_with_multiple_rules_then_combiner_depends_on_aliasing_not_key_extraction(
        self, assembly: WorkflowVersionAssembly, default_key_path: str
    ) -> None:
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={},
        )
        kuiper1 = AliasingKuiper(expression="expr1")

        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={},
        )
        kuiper2 = AliasingKuiper(expression="expr2")

        result = assembly.build(
            WorkflowVersionAssemblyRequest(
                rule_kuiper_pairs=[(rule1, kuiper1), (rule2, kuiper2)], key_path=default_key_path
            )
        )

        yaml_text = result.workflow_version_yaml
        combiner_section_start = yaml_text.find("externalId: combiner_task")
        combiner_section_end = yaml_text.find("\n    - externalId:", combiner_section_start + 1)
        if combiner_section_end == -1:
            combiner_section = yaml_text[combiner_section_start:]
        else:
            combiner_section = yaml_text[combiner_section_start:combiner_section_end]

        assert "- externalId: aliasing_task_rule1" in combiner_section
        assert "- externalId: aliasing_task_rule2" in combiner_section
        assert "- externalId: key_extraction_task" not in combiner_section

    def test_when_build_called_then_key_extraction_task_has_required_parameters(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
        default_key_path: str,
    ) -> None:
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=default_key_path)
        )

        key_extraction_start = result.workflow_version_yaml.find("externalId: key_extraction_task")
        next_task_start = result.workflow_version_yaml.find("\n    - externalId:", key_extraction_start + 1)
        if next_task_start == -1:
            key_extraction_section = result.workflow_version_yaml[key_extraction_start:]
        else:
            key_extraction_section = result.workflow_version_yaml[key_extraction_start:next_task_start]

        assert "type: jsonMapping" in key_extraction_section
        assert "retries: 0" in key_extraction_section
        assert "onFailure: abortWorkflow" in key_extraction_section
        assert "jsonMapping:" in key_extraction_section
        assert "inputs:" in key_extraction_section
        assert "- input" in key_extraction_section

    def test_when_build_called_with_custom_key_path_then_uses_custom_path(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        custom_key_path = "properties.custom_field.nested_prop"
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=custom_key_path)
        )

        assert "properties.custom_field.nested_prop" in result.workflow_version_yaml

    def test_when_build_called_with_complex_key_path_then_preserves_special_characters(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        complex_key_path = 'properties.cdf_cdm["CustomAsset/v2"].name'
        result = assembly.build(
            WorkflowVersionAssemblyRequest(rule_kuiper_pairs=[(simple_rule, simple_kuiper)], key_path=complex_key_path)
        )

        assert "CustomAsset/v2" in result.workflow_version_yaml
        assert "properties.cdf_cdm" in result.workflow_version_yaml
