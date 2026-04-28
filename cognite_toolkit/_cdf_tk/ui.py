from collections.abc import Iterable, Sequence
from typing import ClassVar, Literal

import questionary
from rich import box as rich_box
from rich.console import Console, ConsoleOptions, Group, JustifyMethod, RenderableType, RenderResult
from rich.padding import Padding, PaddingDimensions
from rich.panel import Panel
from rich.style import StyleType
from rich.table import Column, Table
from rich.text import Text

__all__ = [
    "QUESTIONARY_STYLE",
    "ToolkitPanel",
    "ToolkitPanelSection",
    "ToolkitTable",
    "hanging_indent",
]


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

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield ""
        yield from super().__rich_console__(console, options)


class ToolkitPanelSection(Group):
    _nested_padding: ClassVar[PaddingDimensions] = (0, 0, 1, 2)

    def __init__(
        self,
        title: str | Text | None = None,
        description: str | Text | None = None,
        content: Sequence[RenderableType] | None = None,
    ) -> None:
        renderables: list[RenderableType] = []
        header = f"[bold]{title}:[/]" if title else ""
        if description:
            header = f"{header} {description}".strip()

        if header:
            renderables.append(header)
        for item in content or []:
            if isinstance(item, ToolkitPanelSection):
                renderables.append(Padding(item, self._nested_padding))
            else:
                renderables.append(item)

        super().__init__(*renderables)


def hanging_indent(marker: str, text: RenderableType, *, marker_style: StyleType = "none") -> RenderableType:
    grid = Table.grid(padding=(0, 1))
    grid.add_column(no_wrap=True, style=marker_style)
    grid.add_column(ratio=1, overflow="fold")
    grid.add_row(marker, text)
    return grid


class ToolkitTable(Table):
    def __init__(
        self,
        *headers: Column | str,
        title: str | Text | None = None,
        caption: str | Text | None = None,
        width: int | None = None,
        min_width: int | None = None,
        box: rich_box.Box | None = rich_box.SIMPLE,
        safe_box: bool | None = None,
        padding: PaddingDimensions = (0, 1),
        collapse_padding: bool = False,
        pad_edge: bool = True,
        expand: bool = False,
        show_header: bool = True,
        show_footer: bool = False,
        show_edge: bool = False,
        show_lines: bool = False,
        leading: int = 0,
        style: StyleType = "none",
        row_styles: Iterable[StyleType] | None = None,
        header_style: StyleType | None = "table.header",
        footer_style: StyleType | None = "table.footer",
        border_style: StyleType | None = None,
        title_style: StyleType | None = None,
        caption_style: StyleType | None = None,
        title_justify: JustifyMethod = "left",
        caption_justify: JustifyMethod = "left",
        highlight: bool = False,
    ) -> None:
        super().__init__(
            *headers,
            title=title,
            caption=caption,
            width=width,
            min_width=min_width,
            box=box,
            safe_box=safe_box,
            padding=padding,
            collapse_padding=collapse_padding,
            pad_edge=pad_edge,
            expand=expand,
            show_header=show_header,
            show_footer=show_footer,
            show_edge=show_edge,
            show_lines=show_lines,
            leading=leading,
            style=style,
            row_styles=row_styles,
            header_style=header_style,
            footer_style=footer_style,
            border_style=border_style,
            title_style=title_style,
            caption_style=caption_style,
            title_justify=title_justify,
            caption_justify=caption_justify,
            highlight=highlight,
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
