"""Checkbox prompt where the checked row tracks the keyboard highlight.

Implementation module for :meth:`ToolkitQuestion.checkbox_follow_pointer`.
"""

import string
from collections.abc import Callable, Sequence
from typing import Any, Union

from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.styles import Style
from questionary import utils
from questionary.constants import DEFAULT_QUESTION_PREFIX, DEFAULT_SELECTED_POINTER, INVALID_INPUT
from questionary.prompts import common
from questionary.prompts.common import Choice, InquirerControl, Separator
from questionary.question import Question
from questionary.styles import merge_styles_default


class PointerSyncedInquirerControl(InquirerControl):
    """Keep ``selected_options`` equal to the highlighted row after each move."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.is_selection_valid():
            self._sync_selection_to_pointer()

    def select_next(self) -> None:
        super().select_next()
        self._sync_selection_to_pointer()

    def select_previous(self) -> None:
        super().select_previous()
        self._sync_selection_to_pointer()

    def _sync_selection_to_pointer(self) -> None:
        if not self.is_selection_valid():
            return
        choice = self.get_pointed_at()
        self.selected_options = [choice.value]


def _checkbox_follow_pointer(
    message: str,
    choices: Sequence[Union[str, Choice, dict[str, Any]]],
    default: str | None = None,
    validate: Callable[[list[str]], Union[bool, str]] = lambda a: True,
    qmark: str = DEFAULT_QUESTION_PREFIX,
    pointer: str | None = DEFAULT_SELECTED_POINTER,
    style: Style | None = None,
    initial_choice: Union[str, Choice, dict[str, Any]] | None = None,
    use_arrow_keys: bool = True,
    use_jk_keys: bool = True,
    use_emacs_keys: bool = True,
    use_search_filter: Union[str, bool, None] = False,
    instruction: str | None = None,
    show_description: bool = True,
    **kwargs: Any,
) -> Question:
    """Like ``questionary.checkbox``, but moving the pointer updates the selection."""
    if not (use_arrow_keys or use_jk_keys or use_emacs_keys):
        raise ValueError("Some option to move the selection is required. Arrow keys or j/k or Emacs keys.")

    if use_jk_keys and use_search_filter:
        raise ValueError("Cannot use j/k keys with prefix filter search, since j/k can be part of the prefix.")

    merged_style = merge_styles_default(
        [
            Style([("bottom-toolbar", "noreverse")]),
            style,
        ]
    )

    if not callable(validate):
        raise ValueError("validate must be callable")

    ic = PointerSyncedInquirerControl(
        choices,
        default,
        pointer=pointer,
        initial_choice=initial_choice,
        show_description=show_description,
    )

    def get_prompt_tokens() -> list[tuple[str, str]]:
        tokens = []

        tokens.append(("class:qmark", qmark))
        tokens.append(("class:question", f" {message} "))

        if ic.is_answered:
            nbr_selected = len(ic.selected_options)
            if nbr_selected == 0:
                tokens.append(("class:answer", "done"))
            elif nbr_selected == 1:
                if isinstance(ic.get_selected_values()[0].title, list):
                    ts = ic.get_selected_values()[0].title
                    tokens.append(
                        (
                            "class:answer",
                            "".join([token[1] for token in ts]),  # type:ignore
                        )
                    )
                else:
                    tokens.append(
                        (
                            "class:answer",
                            f"[{ic.get_selected_values()[0].title}]",
                        )
                    )
            else:
                tokens.append(("class:answer", f"done ({nbr_selected} selections)"))
        else:
            if instruction is not None:
                tokens.append(("class:instruction", instruction))
            else:
                tokens.append(
                    (
                        "class:instruction",
                        "(Arrow keys move; highlighted row is selected; "
                        "<space> toggles; "
                        f"<{'ctrl-a' if use_search_filter else 'a'}> all; "
                        f"<{'ctrl-i' if use_search_filter else 'i'}> invert"
                        f"{', type to filter' if use_search_filter else ''})",
                    )
                )
        return tokens

    def get_selected_values() -> list[Any]:
        return [c.value for c in ic.get_selected_values()]

    def perform_validation(selected_values: list[str]) -> bool:
        verdict = validate(selected_values)
        valid = verdict is True

        if not valid:
            if verdict is False:
                error_text = INVALID_INPUT
            else:
                error_text = str(verdict)

            error_message = FormattedText([("class:validation-toolbar", error_text)])

        ic.error_message = (
            error_message if not valid and ic.submission_attempted else None  # type: ignore[assignment]
        )

        return valid

    layout = common.create_inquirer_layout(ic, get_prompt_tokens, **kwargs)

    bindings = KeyBindings()

    @bindings.add(Keys.ControlQ, eager=True)
    @bindings.add(Keys.ControlC, eager=True)
    def _(event):
        event.app.exit(exception=KeyboardInterrupt, style="class:aborting")

    @bindings.add(" ", eager=True)
    def toggle(_event):
        pointed_choice = ic.get_pointed_at().value
        if pointed_choice in ic.selected_options:
            ic.selected_options.remove(pointed_choice)
        else:
            ic.selected_options.append(pointed_choice)

        perform_validation(get_selected_values())

    @bindings.add(Keys.ControlI if use_search_filter else "i", eager=True)
    def invert(_event):
        inverted_selection = [
            c.value
            for c in ic.choices
            if not isinstance(c, Separator) and c.value not in ic.selected_options and not c.disabled
        ]
        ic.selected_options = inverted_selection

        perform_validation(get_selected_values())

    @bindings.add(Keys.ControlA if use_search_filter else "a", eager=True)
    def all(_event):
        all_selected = True
        for c in ic.choices:
            if not isinstance(c, Separator) and c.value not in ic.selected_options and not c.disabled:
                ic.selected_options.append(c.value)
                all_selected = False
        if all_selected:
            ic.selected_options = []

        perform_validation(get_selected_values())

    def move_cursor_down(event):
        ic.select_next()
        while not ic.is_selection_valid():
            ic.select_next()

    def move_cursor_up(event):
        ic.select_previous()
        while not ic.is_selection_valid():
            ic.select_previous()

    if use_search_filter:

        def search_filter(event):
            ic.add_search_character(event.key_sequence[0].key)

        for character in string.printable:
            if character in string.whitespace:
                continue
            bindings.add(character, eager=True)(search_filter)
        bindings.add(Keys.Backspace, eager=True)(search_filter)

    if use_arrow_keys:
        bindings.add(Keys.Down, eager=True)(move_cursor_down)
        bindings.add(Keys.Up, eager=True)(move_cursor_up)

    if use_jk_keys:
        bindings.add("j", eager=True)(move_cursor_down)
        bindings.add("k", eager=True)(move_cursor_up)

    if use_emacs_keys:
        bindings.add(Keys.ControlN, eager=True)(move_cursor_down)
        bindings.add(Keys.ControlP, eager=True)(move_cursor_up)

    @bindings.add(Keys.ControlM, eager=True)
    def set_answer(event):
        selected_values = get_selected_values()
        ic.submission_attempted = True

        if perform_validation(selected_values):
            ic.is_answered = True
            event.app.exit(result=selected_values)

    @bindings.add(Keys.Any)
    def other(_event):
        """Disallow inserting other text."""

    return Question(
        Application(
            layout=layout,
            key_bindings=bindings,
            style=merged_style,
            **utils.used_kwargs(kwargs, Application.__init__),
        )
    )
