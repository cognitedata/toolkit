import re
from collections.abc import Hashable

from rich.console import Console

from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning

INVALID_TITLE_REGEX = re.compile(r"[\\*?:/\[\]]")


def suffix_description(
    suffix: str,
    description: str | None,
    description_character_limit: int,
    identifier: Hashable,
    resource_type: str,
    console: Console | None = None,
) -> str:
    """Appends a suffix to a description if it is not already present.
    If the description is too long after appending the suffix, it will be truncated to fit within the character limit.

    Args:
        suffix: The suffix to append to the description.
        description: The original description to which the suffix will be appended.
        description_character_limit: The maximum number of characters allowed in the description after appending the suffix.
        identifier: Hashable identifier for the resource, used in warnings.
        resource_type:  Type of the resource, used in warnings.
        console: Console object for printing warnings.

    Returns:
        str: The modified description with the suffix appended, or truncated if necessary.
    """
    if description is None or description == "":
        return suffix
    elif description.endswith(suffix):
        # The suffix is already in the description
        return description
    elif len(description) + len(suffix) + 1 < description_character_limit:
        return f"{description} {suffix}"
    else:
        LowSeverityWarning(f"Description is too long for {resource_type} {identifier!r}. Truncating...").print_warning(
            console=console
        )
        truncation = description_character_limit - len(suffix) - 3
        return f"{description[:truncation]}...{suffix}"


def sanitize_spreadsheet_title(title: str) -> str:
    """Sanitizes a title for use in a spreadsheet by removing invalid characters.

    Args:
        title: The original title to sanitize.

    Returns:
        str: The sanitized title with invalid characters removed.
    """
    if not title:
        return "Sheet"
    sanitized_title = INVALID_TITLE_REGEX.sub("", title)
    return sanitized_title if sanitized_title else "Sheet"
