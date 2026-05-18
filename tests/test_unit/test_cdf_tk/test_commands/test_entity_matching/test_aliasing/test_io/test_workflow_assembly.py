import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.workflow_assembly import (
    WorkflowBundle,
    WorkflowVersionAssembly,
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
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert isinstance(result, WorkflowBundle)
        assert result.workflow_yaml is not None
        assert result.workflow_version_yaml is not None

    def test_when_build_called_then_includes_default_external_id(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "externalId: entity_matching_aliasing" in result.workflow_yaml

    def test_when_build_called_then_includes_default_workflow_description(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "description: Entity matching aliasing workflow" in result.workflow_yaml

    def test_when_build_called_with_custom_external_id_then_uses_custom_id(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        custom_id = "my_custom_workflow"
        result = assembly.build(
            [(simple_rule, simple_kuiper)],
            workflow_external_id=custom_id,
        )

        assert f"externalId: {custom_id}" in result.workflow_yaml
        assert f"workflowExternalId: {custom_id}" in result.workflow_version_yaml

    def test_when_build_called_with_custom_description_then_uses_custom_description(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        custom_description = "My custom workflow description"
        result = assembly.build(
            [(simple_rule, simple_kuiper)],
            workflow_description=custom_description,
        )

        assert f"description: {custom_description}" in result.workflow_yaml

    def test_when_build_called_with_single_rule_then_creates_single_task(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "aliasing_task_simple_rule" in result.workflow_version_yaml
        assert "jsonMapping" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_rule_name(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert f"name: {simple_rule.name}" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_rule_description(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert f"description: {simple_rule.description}" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_kuiper_expression(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        escaped_expression = simple_kuiper.expression.replace('"', '\\"')
        assert escaped_expression in result.workflow_version_yaml

    def test_when_build_called_with_expression_containing_quotes_then_escapes_quotes(
        self,
        assembly: WorkflowVersionAssembly,
        rule_with_special_chars: AliasingRule,
        kuiper_with_special_chars: AliasingKuiper,
    ) -> None:
        result = assembly.build([(rule_with_special_chars, kuiper_with_special_chars)])

        escaped_expression = kuiper_with_special_chars.expression.replace('"', '\\"')
        assert escaped_expression in result.workflow_version_yaml

    def test_when_build_called_with_multiple_rules_then_creates_multiple_tasks(
        self,
        assembly: WorkflowVersionAssembly,
        simple_rule: AliasingRule,
        simple_kuiper: AliasingKuiper,
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

        result = assembly.build([(rule1, kuiper1), (rule2, kuiper2)])

        assert "aliasing_task_rule_one" in result.workflow_version_yaml
        assert "aliasing_task_rule_two" in result.workflow_version_yaml
        assert "name: rule_one" in result.workflow_version_yaml
        assert "name: rule_two" in result.workflow_version_yaml

    def test_when_build_called_then_workflow_version_includes_required_fields(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "workflowExternalId: entity_matching_aliasing" in result.workflow_version_yaml
        assert "version: v1" in result.workflow_version_yaml
        assert "workflowDefinition:" in result.workflow_version_yaml
        assert "description: Entity matching aliasing workflow" in result.workflow_version_yaml
        assert "tasks:" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_required_parameters(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "type: jsonMapping" in result.workflow_version_yaml
        assert "retries: 0" in result.workflow_version_yaml
        assert "onFailure: abortWorkflow" in result.workflow_version_yaml
        assert "jsonMapping:" in result.workflow_version_yaml
        assert "inputs:" in result.workflow_version_yaml
        assert "- input" in result.workflow_version_yaml

    def test_when_build_called_then_task_includes_workflow_input_reference(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        assert "${workflow.input}" in result.workflow_version_yaml

    def test_when_build_workflow_called_with_defaults_then_returns_valid_yaml(
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        result = assembly._build_workflow()

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
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly._build_workflow_version([(simple_rule, simple_kuiper)])

        assert "aliasing_task_simple_rule" in result
        assert f"name: {simple_rule.name}" in result

    def test_when_build_workflow_version_called_with_multiple_rules_then_all_tasks_present(
        self, assembly: WorkflowVersionAssembly
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

        result = assembly._build_workflow_version([(rule1, kuiper1), (rule2, kuiper2), (rule3, kuiper3)])

        assert "aliasing_task_rule1" in result
        assert "aliasing_task_rule2" in result
        assert "aliasing_task_rule3" in result
        assert "name: rule1" in result
        assert "name: rule2" in result
        assert "name: rule3" in result

    def test_when_build_workflow_version_called_then_external_id_derived_from_workflow_id(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        custom_workflow_id = "my_workflow"
        result = assembly._build_workflow_version(
            [(simple_rule, simple_kuiper)], workflow_external_id=custom_workflow_id
        )

        assert f"workflowExternalId: {custom_workflow_id}" in result

    def test_when_expression_contains_quotes_then_properly_escaped_in_task(
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        rule = AliasingRule(
            name="test_rule",
            rule_type="regex_substitution",
            description="Test rule with quotes",
            payload={},
        )
        kuiper = AliasingKuiper(expression='test.replace("old", "new")')

        result = assembly._build_workflow_version([(rule, kuiper)])

        escaped_expression = 'test.replace(\\"old\\", \\"new\\")'
        assert escaped_expression in result

    def test_when_expression_has_mixed_quotes_then_only_double_quotes_escaped(
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="Test rule",
            payload={},
        )
        kuiper = AliasingKuiper(expression="test.replace('single', \"double\")")

        result = assembly._build_workflow_version([(rule, kuiper)])

        escaped_expression = "test.replace('single', \\\"double\\\")"
        assert escaped_expression in result

    def test_when_build_called_then_workflow_yaml_properly_formatted(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

        lines = result.workflow_yaml.strip().split("\n")
        assert len(lines) >= 2
        assert lines[0].startswith("externalId:")
        assert lines[1].startswith("description:")

    def test_when_build_called_then_workflow_version_yaml_properly_indented(
        self, assembly: WorkflowVersionAssembly, simple_rule: AliasingRule, simple_kuiper: AliasingKuiper
    ) -> None:
        result = assembly.build([(simple_rule, simple_kuiper)])

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
        self, assembly: WorkflowVersionAssembly
    ) -> None:
        rule = AliasingRule(
            name="rule_with_underscores_name",
            rule_type="character_substitution",
            description="Test",
            payload={},
        )
        kuiper = AliasingKuiper(expression="test_expr")

        result = assembly.build([(rule, kuiper)])

        assert "aliasing_task_rule_with_underscores_name" in result.workflow_version_yaml
