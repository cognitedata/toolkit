from io import StringIO
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.commands.deploy_v2.diff_display import (
    DeployDiffFormat,
    _align_nested_dict_pair_for_yaml,
    render_deploy_human_diff,
)


def test_deploy_diff_format_enum_value() -> None:
    assert DeployDiffFormat.human.value == "human"


def test_align_nested_dict_pair_preserves_build_key_order_then_cdf_only() -> None:
    cdf = {"z": 1, "a": 2, "only_cdf": 0}
    build = {"a": 2, "z": 9}
    c2, b2 = _align_nested_dict_pair_for_yaml(cdf, build)
    assert list(c2.keys()) == list(b2.keys())
    assert list(c2.keys()) == ["a", "z", "only_cdf"]
    assert c2["only_cdf"] == 0
    assert b2["only_cdf"] is None


def test_align_nested_dict_pair_aligns_nested_dict_key_order() -> None:
    cdf = {"top": {"m": 1, "n": 2}}
    build = {"top": {"n": 2, "m": 1}}
    c2, b2 = _align_nested_dict_pair_for_yaml(cdf, build)
    assert list(c2["top"].keys()) == list(b2["top"].keys())
    assert list(c2["top"].keys()) == ["n", "m"]


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
