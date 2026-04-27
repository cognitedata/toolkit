from typing import Literal

import questionary
from rich import box as rich_box
from rich.console import RenderableType
from rich.panel import Panel
from rich.style import StyleType
from rich.text import Text

__all__ = ["QUESTIONARY_STYLE", "ToolkitPanel"]


class ToolkitPanel(Panel):
    def __init__(
        self,
        renderable: RenderableType,
        box: rich_box.Box = rich_box.ROUNDED,
        *,
        title: str | Text | None = None,
        title_align: Literal["left", "center", "right"] = "left",
        subtitle: str | Text | None = None,
        subtitle_align: Literal["left", "center", "right"] = "left",
        safe_box: bool | None = None,
        expand: bool = True,
        style: StyleType = "none",
        border_style: StyleType = "none",
        width: int | None = None,
        height: int | None = None,
        padding: int | tuple[int] | tuple[int, int] | tuple[int, int, int, int] = (1, 2),
        highlight: bool = False,
    ) -> None:
        super().__init__(
            renderable,
            box,
            title=title,
            title_align=title_align,
            subtitle=subtitle,
            subtitle_align=subtitle_align,
            safe_box=safe_box,
            expand=expand,
            style=style,
            border_style=border_style,
            width=width,
            height=height,
            padding=padding,
            highlight=highlight,
        )


QUESTIONARY_STYLE = questionary.Style(
    [
        ("qmark", "fg:#673ab7"),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", "fg:#f44336 bold"),  # submitted answer text behind the question
        ("pointer", "fg:#673ab7 bold"),  # pointer used in select and checkbox prompts
        ("highlighted", "fg:#673ab7 bold"),  # pointed-at choice in select and checkbox prompts
        ("selected", "fg:#673ab7"),  # style for a selected item of a checkbox
        ("separator", "fg:#cc5454"),  # separator in lists
        ("instruction", ""),  # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        ("disabled", "fg:#858585 italic"),  # disabled choices for select and checkbox prompts
    ]
)
