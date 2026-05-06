from collections.abc import Sequence
from enum import Enum
from typing import Any, ClassVar, Literal

import questionary
from rich import box as rich_box
from rich.console import Console, ConsoleOptions, Group, JustifyMethod, RenderableType, RenderResult
from rich.padding import Padding, PaddingDimensions
from rich.panel import Panel
from rich.style import StyleType
from rich.table import Table
from rich.text import Text

__all__ = [
    "QUESTIONARY_STYLE",
    "AuraColor",
    "ToolkitPanel",
    "ToolkitPanelSection",
    "ToolkitTable",
    "hanging_indent",
]


# https://cognitedata.github.io/aura/primitives/colors
class AuraColor(str, Enum):
    NORDIC = "#1a967a"
    FJORD = "#5f7def"
    DUSK = "#b765c4"
    AURORA = "#649210"
    SKY = "#0290b6"
    RED = "#f43d5c"
    AMBER = "#FCB100"
    GREEN = "#1c984a"
    MOUNTAIN = "#7c868e"

    @property
    def fg(self) -> str:
        return f"fg:{self.value}"

    @property
    def rich(self) -> str:
        return self.value


class ToolkitPanel(Panel):
    """Branded Rich :class:`~rich.panel.Panel` for toolkit CLI output (leading newline, default rounding).

    Constructor arguments are forwarded to :class:`~rich.panel.Panel` after normalizing string
    ``title`` to bold :class:`~rich.text.Text`.

    The ``padding`` argument controls empty space **inside** the panel border, around the main
    ``renderable``. It uses the same rules as Rich :class:`~rich.padding.Padding`:

    - **int** — same padding on all four sides.
    - **pair** ``(vertical, horizontal)`` — top/bottom vs left/right.
    - **4-tuple** ``(top, right, bottom, left)`` — explicit per side, in clockwise order from the top.

    Any remaining keyword arguments are passed through to :class:`~rich.panel.Panel` (e.g.
    ``border_style``, ``expand``).
    """

    def __init__(
        self,
        renderable: RenderableType,
        box: rich_box.Box = rich_box.ROUNDED,
        *,
        title: str | Text | None = None,
        title_align: Literal["left", "center", "right"] = "left",
        subtitle_align: Literal["left", "center", "right"] = "left",
        padding: int | tuple[int] | tuple[int, int] | tuple[int, int, int, int] = (1, 2),
        **kwargs: Any,
    ) -> None:
        if isinstance(title, str):
            title = Text.from_markup(title, style="bold")
        super().__init__(
            renderable,
            box,
            title=title,
            title_align=title_align,
            subtitle_align=subtitle_align,
            padding=padding,
            **kwargs,
        )

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield ""
        yield from super().__rich_console__(console, options)


class ToolkitPanelSection(Group):
    """A titled group of renderables, typically nested inside a :class:`ToolkitPanel`.

    Renders an optional bold ``title`` line (with trailing colon), optional ``description`` on
    the same line, then the ``content`` items. Nested :class:`ToolkitPanelSection` instances are
    indented with a fixed inset (:attr:`_nested_padding`).

    Args:
        title: Optional section heading. When set, shown as ``"{title}:"`` in bold markup.
        description: Optional text joined to the header after ``title``.
        content: Renderables listed under the header (strings are rendered as markup).
        content_padding: Optional extra space around the **body** only (everything under the
            header). The header line is never padded. Uses Rich :class:`~rich.padding.Padding`
            dimensions:

            - **int** — pad all sides by that many cells.
            - **pair** ``(vertical, horizontal)`` — top/bottom vs left/right.
            - **4-tuple** ``(top, right, bottom, left)`` — explicit per side (clockwise from top).

            For example, ``(0, 0, 0, 1)`` is one cell of **left** inset; ``(0, 0, 1, 0)`` is one row
            of **bottom** spacing (third value is bottom, fourth is left).
    """

    _nested_padding: ClassVar[PaddingDimensions] = (0, 0, 1, 2)

    def __init__(
        self,
        title: str | Text | None = None,
        description: str | Text | None = None,
        content: Sequence[RenderableType] | None = None,
        content_padding: PaddingDimensions | None = None,
    ) -> None:
        renderables: list[RenderableType] = []
        header = f"[bold]{title}:[/]" if title else ""
        if description:
            header = f"{header} {description}".strip()

        if header:
            renderables.append(header)
        body_items: list[RenderableType] = []
        for item in content or []:
            if isinstance(item, ToolkitPanelSection):
                body_items.append(Padding(item, self._nested_padding))
            else:
                body_items.append(item)
        if body_items:
            body: RenderableType = body_items[0] if len(body_items) == 1 else Group(*body_items)
            if content_padding is not None:
                body = Padding(body, content_padding)
            renderables.append(body)

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
        ("qmark", AuraColor.FJORD.fg),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", f"{AuraColor.NORDIC.fg} bold"),  # submitted answer text behind the question
        ("pointer", f"{AuraColor.FJORD.fg} bold"),  # pointer used in select and checkbox prompts
        ("highlighted", f"{AuraColor.FJORD.fg} bold"),  # pointed-at choice in select and checkbox prompts
        ("selected", AuraColor.NORDIC.fg),  # style for a selected item of a checkbox
        ("separator", AuraColor.MOUNTAIN.fg),  # separator in lists
        ("instruction", ""),  # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        ("disabled", f"{AuraColor.MOUNTAIN.fg} italic"),  # disabled choices for select and checkbox prompts
    ]
)
