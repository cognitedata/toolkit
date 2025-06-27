from collections.abc import Hashable

from rich.console import Console

from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning


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
