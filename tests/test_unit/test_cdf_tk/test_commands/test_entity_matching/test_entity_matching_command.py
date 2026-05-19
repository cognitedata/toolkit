from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching import EntityMatchingCommand
from cognite_toolkit._cdf_tk.constants import MODULES


def _make_module(base: Path) -> Path:
    module_dir = base / MODULES / "my_module"
    (module_dir / "data_sets").mkdir(parents=True, exist_ok=True)
    return module_dir


def _make_rules_yaml(path: Path, rules: list[dict]) -> Path:
    import yaml

    rules_data = {"rules": rules}
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(rules_data, f)
    return path


@pytest.fixture
def cmd() -> EntityMatchingCommand:
    return EntityMatchingCommand(print_warning=False, skip_tracking=True, silent=True)


@pytest.fixture
def single_rule() -> dict:
    return {
        "name": "test_rule",
        "rule_type": "character_substitution",
        "description": "Test rule",
        "payload": {"replacements": {"P": "K"}},
    }


@pytest.fixture
def multiple_rules() -> list[dict]:
    return [
        {
            "name": "rule_1",
            "rule_type": "character_substitution",
            "description": "First test rule",
            "payload": {"replacements": {"P": "K"}},
        },
        {
            "name": "rule_2",
            "rule_type": "character_substitution",
            "description": "Second test rule",
            "payload": {"replacements": {"C": "D"}},
        },
    ]


@pytest.fixture
def mock_kuiper() -> AliasingKuiper:
    kuiper = MagicMock(spec=AliasingKuiper)
    kuiper.expression = "input.map(item => item.key)"
    return kuiper


class TestEntityMatchingCommand:
    def test_generate_aliasing_workflow_success_single_rule(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_rule: dict,
        mock_kuiper: AliasingKuiper,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_rule])

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=MagicMock(generate=MagicMock(return_value=mock_kuiper)),
            ),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        workflow_dir = module_path / "workflows"
        assert workflow_dir.exists()

        workflow_path = workflow_dir / "rules.Workflow.yaml"
        workflow_version_path = workflow_dir / "rules.WorkflowVersion.yaml"

        assert workflow_path.exists()
        assert workflow_version_path.exists()

        workflow_content = workflow_path.read_text()
        assert "externalId: entity_matching_aliasing" in workflow_content
        assert "description: Entity matching aliasing workflow" in workflow_content

        workflow_version_content = workflow_version_path.read_text()
        assert "workflowExternalId: entity_matching_aliasing" in workflow_version_content
        assert "version: v1" in workflow_version_content
        assert "aliasing_task_test_rule" in workflow_version_content

    def test_generate_aliasing_workflow_multiple_rules(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        multiple_rules: list[dict],
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", multiple_rules)

        mock_facade = MagicMock()
        mock_facade.generate.side_effect = [
            MagicMock(spec=AliasingKuiper, expression="rule_1_expression"),
            MagicMock(spec=AliasingKuiper, expression="rule_2_expression"),
        ]

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=mock_facade,
            ),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"
        workflow_version_content = workflow_version_path.read_text()

        assert "aliasing_task_rule_1" in workflow_version_content
        assert "aliasing_task_rule_2" in workflow_version_content
        assert "combiner_task" in workflow_version_content
        assert workflow_version_content.count("jsonMapping:") == 3

    def test_generate_aliasing_workflow_input_file_not_found(
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
                module_name="my_module",
                organization_dir=org_dir,
            )

    def test_generate_aliasing_workflow_creates_workflows_directory(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_rule: dict,
        mock_kuiper: AliasingKuiper,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_rule])

        workflows_dir = module_path / "workflows"
        assert not workflows_dir.exists()

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=MagicMock(generate=MagicMock(return_value=mock_kuiper)),
            ),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        assert workflows_dir.exists()

    def test_generate_aliasing_workflow_uses_input_stem_for_output_files(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_rule: dict,
        mock_kuiper: AliasingKuiper,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "my_aliasing_rules.yaml", [single_rule])

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=MagicMock(generate=MagicMock(return_value=mock_kuiper)),
            ),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        workflow_dir = module_path / "workflows"
        workflow_path = workflow_dir / "my_aliasing_rules.Workflow.yaml"
        workflow_version_path = workflow_dir / "my_aliasing_rules.WorkflowVersion.yaml"

        assert workflow_path.exists()
        assert workflow_version_path.exists()

    def test_generate_aliasing_workflow_console_output(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_rule: dict,
        mock_kuiper: AliasingKuiper,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_rule])

        console_messages = []

        def mock_console(msg: str) -> None:
            console_messages.append(msg)

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=MagicMock(generate=MagicMock(return_value=mock_kuiper)),
            ),
            patch.object(cmd, "console", side_effect=mock_console),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        assert len(console_messages) >= 2
        assert any("test_rule" in msg for msg in console_messages)
        assert any("rules.Workflow.yaml" in msg for msg in console_messages)
        assert any("rules.WorkflowVersion.yaml" in msg for msg in console_messages)

    def test_generate_aliasing_workflow_escapes_quotes_in_expressions(
        self,
        cmd: EntityMatchingCommand,
        tmp_path: Path,
        single_rule: dict,
    ) -> None:
        org_dir = tmp_path / "org"
        module_path = _make_module(org_dir)

        rules_yaml = _make_rules_yaml(tmp_path / "rules.yaml", [single_rule])

        mock_kuiper = MagicMock(spec=AliasingKuiper)
        mock_kuiper.expression = 'input | SELECT "field"'

        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.ModuleResolver.get_or_prompt_module_path",
                return_value=module_path,
            ),
            patch(
                "cognite_toolkit._cdf_tk.commands.entity_matching.entity_matching.provide_aliasing_facade",
                return_value=MagicMock(generate=MagicMock(return_value=mock_kuiper)),
            ),
        ):
            cmd.generate_aliasing_workflow(
                input_yaml=rules_yaml,
                module_name="my_module",
                organization_dir=org_dir,
            )

        workflow_version_path = module_path / "workflows" / "rules.WorkflowVersion.yaml"
        workflow_version_content = workflow_version_path.read_text()

        assert '\\"field\\"' in workflow_version_content
