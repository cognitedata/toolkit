from collections.abc import Sequence
from enum import Enum
from typing import Any, ClassVar, Literal, cast

import questionary
import questionary.constants as qc
import questionary.styles as qstyles
from prompt_toolkit.styles import Style as PTStyle
from prompt_toolkit.styles import merge_styles
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
    "apply_questionary_toolkit_defaults",
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


# "reverse" keeps the active row visible when truecolor (fg:#...) is ignored or low-contrast
# (macOS Terminal / iTerm2); see CDF-27852.
_QUESTIONARY_POINTER_HIGHLIGHT = f"reverse bold {AuraColor.FJORD.fg}"

QUESTIONARY_STYLE = questionary.Style(
    [
        ("qmark", AuraColor.FJORD.fg),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", f"{AuraColor.NORDIC.fg} bold"),  # submitted answer text behind the question
        ("pointer", _QUESTIONARY_POINTER_HIGHLIGHT),  # pointer used in select and checkbox prompts
        ("highlighted", _QUESTIONARY_POINTER_HIGHLIGHT),  # pointed-at choice in select and checkbox prompts
        ("selected", AuraColor.NORDIC.fg),  # style for a selected item of a checkbox
        ("separator", AuraColor.MOUNTAIN.fg),  # separator in lists
        ("instruction", ""),  # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        ("disabled", f"{AuraColor.MOUNTAIN.fg} italic"),  # disabled choices for select and checkbox prompts
    ]
)


def apply_questionary_toolkit_defaults() -> None:
    """Merge Toolkit questionary styles into library defaults for every prompt."""
    merged = cast(PTStyle, merge_styles([qc.DEFAULT_STYLE, QUESTIONARY_STYLE]))
    qc.DEFAULT_STYLE = merged
    qstyles.DEFAULT_STYLE = merged
