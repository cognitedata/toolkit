from rich import box

from cognite_toolkit._cdf_tk.ui import ToolkitPanel


def test_toolkit_panel_defaults_title_align_left_and_expand_true() -> None:
    panel = ToolkitPanel("content", title="Title")

    assert panel.title_align == "left"
    assert panel.expand is True


def test_toolkit_panel_allows_overriding_defaults() -> None:
    panel = ToolkitPanel("content", box=box.SQUARE, title="Title", title_align="center", expand=False)

    assert panel.box == box.SQUARE
    assert panel.title_align == "center"
    assert panel.expand is False
