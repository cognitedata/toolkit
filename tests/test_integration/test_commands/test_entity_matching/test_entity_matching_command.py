from pathlib import Path

import pytest
import yaml

from cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching import EntityMatchingCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file


def _make_module(base: Path) -> Path:
    module_dir = base / MODULES / "test_module"
    (module_dir / "data_sets").mkdir(parents=True, exist_ok=True)
    return module_dir


def _make_rules_yaml(path: Path, rules: list[dict]) -> Path:
    rules_data = {
        "rules": rules,
        "key_path": "name",
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(rules_data, f)
    return path


@pytest.fixture
def cmd() -> EntityMatchingCommand:
    return EntityMatchingCommand(print_warning=False, skip_tracking=True, silent=True)


@pytest.fixture
def single_character_substitution_rule() -> dict:
    return {
        "name": "test_p_to_k",
        "rule_type": "character_substitution",
        "description": "Test P to K substitution",
        "payload": {"replacements": {"P": "K"}},
    }


@pytest.fixture
def multiple_rule_types() -> list[dict]:
    return [
        {
            "name": "char_sub_rule",
            "rule_type": "character_substitution",
            "description": "Character substitution rule",
            "payload": {"replacements": {"A": "B"}},
        },
        {
            "name": "case_transform_rule",
            "rule_type": "case_transformation",
            "description": "Case transformation rule",
            "payload": {"strategy": "UPPERCASE"},
        },
    ]


class TestEntityMatchingCommandIntegration:
    def test_generate_aliasing_workflow_single_rule_creates_files(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_character_substitution_rule])

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_dir = module_path / "workflows"
        assert workflow_dir.exists(), "Workflows directory should be created"

        workflow_path = workflow_dir / "rules.Workflow.yaml"
        workflow_version_path = workflow_dir / "rules.WorkflowVersion.yaml"

        assert workflow_path.exists(), "Workflow.yaml file should be created"
        assert workflow_version_path.exists(), "WorkflowVersion.yaml file should be created"

    def test_generate_aliasing_workflow_workflow_yaml_structure(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_character_substitution_rule])

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_path = module_path / "workflows" / "rules.Workflow.yaml"
        workflow_content = read_yaml_file(workflow_path, "dict")

        assert "externalId" in workflow_content, "Workflow should have externalId"
        assert workflow_content["externalId"] == "entity_matching_aliasing"
        assert "description" in workflow_content, "Workflow should have description"

    def test_generate_aliasing_workflow_version_yaml_structure(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_character_substitution_rule])

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"
        workflow_version_content = read_yaml_file(workflow_version_path, "dict")

        assert "workflowExternalId" in workflow_version_content
        assert workflow_version_content["workflowExternalId"] == "entity_matching_aliasing"
        assert "version" in workflow_version_content
        assert "workflowDefinition" in workflow_version_content, "WorkflowVersion should have workflowDefinition"
        workflow_def = workflow_version_content["workflowDefinition"]
        assert "tasks" in workflow_def, "workflowDefinition should have tasks"
        assert isinstance(workflow_def["tasks"], list)
        assert len(workflow_def["tasks"]) > 0, "Should have at least one task"

    def test_generate_aliasing_workflow_task_created_for_each_rule(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        multiple_rule_types: list[dict],
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", multiple_rule_types)

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"
        workflow_version_content = read_yaml_file(workflow_version_path, "dict")

        tasks = workflow_version_content["workflowDefinition"]["tasks"]
        assert len(tasks) >= 2, "Should have at least one task per rule"

        task_names = [task.get("name") for task in tasks]
        assert any("char_sub_rule" in str(name) for name in task_names if name), "Should have task for char_sub_rule"
        assert any("case_transform_rule" in str(name) for name in task_names if name), (
            "Should have task for case_transform_rule"
        )

    def test_generate_aliasing_workflow_file_not_found_error(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
    ) -> None:
        org_dir = tmp_path / "org"
        _make_module(org_dir)
        non_existent_file = tmp_path / "non_existent.yaml"

        with pytest.raises(FileNotFoundError, match="Input file not found"):
            cmd.generate_aliasing_workflow(
                input_yaml=non_existent_file,
                module_name="test_module",
                organization_dir=org_dir,
            )

    def test_generate_aliasing_workflow_uses_input_filename_stem(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "my_custom_rules.yaml", [single_character_substitution_rule])

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_dir = module_path / "workflows"
        workflow_path = workflow_dir / "my_custom_rules.Workflow.yaml"
        workflow_version_path = workflow_dir / "my_custom_rules.WorkflowVersion.yaml"

        assert workflow_path.exists(), "Should use custom filename stem for Workflow.yaml"
        assert workflow_version_path.exists(), "Should use custom filename stem for WorkflowVersion.yaml"

    def test_generate_aliasing_workflow_creates_workflows_directory_if_not_exists(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_character_substitution_rule])

        workflows_dir = module_path / "workflows"
        assert not workflows_dir.exists(), "Workflows directory should not exist initially"

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        assert workflows_dir.exists(), "Workflows directory should be created by command"

    def test_generate_aliasing_workflow_yaml_files_are_valid(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_character_substitution_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_character_substitution_rule])

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_path = module_path / "workflows" / "rules.Workflow.yaml"
        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"

        workflow_content = workflow_path.read_text()
        workflow_version_content = workflow_version_path.read_text()

        yaml_workflow = yaml.safe_load(workflow_content)
        yaml_workflow_version = yaml.safe_load(workflow_version_content)

        assert isinstance(yaml_workflow, dict), "Workflow.yaml should be valid YAML dict"
        assert isinstance(yaml_workflow_version, dict), "WorkflowVersion.yaml should be valid YAML dict"

    def test_generate_aliasing_workflow_all_rules_included_in_version(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        multiple_rule_types: list[dict],
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", multiple_rule_types)

        cmd.generate_aliasing_workflow(
            input_yaml=rules_yaml,
            module_name="test_module",
            organization_dir=org_dir,
        )

        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"
        workflow_version_content = workflow_version_path.read_text()

        assert "char_sub_rule" in workflow_version_content
        assert "case_transform_rule" in workflow_version_content
