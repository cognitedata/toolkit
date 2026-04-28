from collections.abc import Sequence
from typing import Any, Literal

import questionary
from rich import box as rich_box
from rich.console import Group, JustifyMethod, RenderableType
from rich.padding import Padding, PaddingDimensions
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

__all__ = ["QUESTIONARY_STYLE", "ToolkitPanel", "ToolkitPanelSection", "ToolkitTable"]


class ToolkitPanel(Panel):
    def __init__(
        self,
        renderable: RenderableType,
        box: rich_box.Box = rich_box.ROUNDED,
        *,
        title_align: Literal["left", "center", "right"] = "left",
        subtitle_align: Literal["left", "center", "right"] = "left",
        padding: int | tuple[int] | tuple[int, int] | tuple[int, int, int, int] = (1, 2),
        **kwargs: Any,
    ) -> None:
        super().__init__(
            renderable,
            box,
            title_align=title_align,
            subtitle_align=subtitle_align,
            padding=padding,
            **kwargs,
        )


class ToolkitPanelSection(Group):
    def __init__(
        self,
        title: str | Text | None = None,
        description: str | Text | None = None,
        content: Sequence[RenderableType] | None = None,
    ) -> None:
        header = f"[bold]{title}:[/]" if title else ""
        if description:
            header = f"{header} {description}".strip()

        renderables: list[RenderableType] = []
        if header:
            renderables.append(header)
        renderables.extend(content or [])

        super().__init__(*renderables)


class ToolkitTable(Table):
    def __init__(
        self,
        *headers: str,
        box: rich_box.Box | None = rich_box.SIMPLE,
        padding: PaddingDimensions = (0, 1),
        expand: bool = False,
        show_edge: bool = False,
        title_justify: JustifyMethod = "left",
        caption_justify: JustifyMethod = "left",
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *headers,
            box=box,
            padding=padding,
            expand=expand,
            show_edge=show_edge,
            title_justify=title_justify,
            caption_justify=caption_justify,
            **kwargs,
        )

    def as_panel_detail(self) -> RenderableType:
        return Padding(self, (1, 0, 1, 2))


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
