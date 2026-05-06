from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.commands.deploy_v2.diff_display import DeployDiffFormat, render_deploy_human_diff


def test_deploy_diff_format_enum_value() -> None:
    assert DeployDiffFormat.human.value == "human"


def test_render_deploy_human_diff_includes_summary_and_columns() -> None:
    panel = render_deploy_human_diff(
        resource_name="Data Sets",
        identifier="my-dataset",
        source_file=Path("modules/foo/datasets.Dataset.yaml"),
        cdf_dict={"name": "a", "token": "SECRET"},
        yaml_dict={"name": "a", "token": "SECRET-other"},
        sensitive_strings=["SECRET"],
    )
    console = Console(record=True, width=120, legacy_windows=False, color_system=None)
    console.print(panel)
    text = console.export_text(clear=False)

    assert "Summary" in text
    assert "CDF (API)" in text
    assert "Build (YAML)" in text
    assert "my-dataset" in text
    assert "modules/foo/datasets.Dataset.yaml" in text
    assert "SECRET" not in text
    assert "********" in text
