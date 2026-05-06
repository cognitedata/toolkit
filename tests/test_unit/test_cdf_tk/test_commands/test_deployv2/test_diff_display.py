from io import StringIO
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
        cdf_project="my-cdf-project",
    )
    buf = StringIO()
    console = Console(file=buf, width=200, legacy_windows=False, color_system=None)
    console.print(panel)
    text = buf.getvalue()

    assert "Summary:" in text
    assert "Diff line by line" not in text
    assert "CDF (my-cdf-project)" in text
    assert "Local build" in text
    assert "my-dataset" in text
    assert "modules/foo/datasets.Dataset.yaml" in text
    assert "SECRET" not in text
    assert "********" in text
