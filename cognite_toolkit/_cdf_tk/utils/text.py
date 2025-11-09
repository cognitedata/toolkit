import hashlib
import re
from collections.abc import Hashable

from rich.console import Console

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
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


def to_sentence_case(text: str) -> str:
    """Converts a string to sentence case.

    Args:
        text: The original string to convert.

    Returns:
        str: The string converted to sentence case.

    >>> to_sentence_case("hello world")
    'hello world'
    >>> to_sentence_case("PYTHON programming")
    'python programming'
    >>> to_sentence_case("")
    ''
    >>> to_sentence_case("snake_case_string")
    'snake case string'
    >>> to_sentence_case("MIXED_case STRING")
    'mixed case string'
    >>> to_sentence_case("camelCaseString")
    'camel case string'
    """
    if not text:
        return text
    # Replace underscores with spaces
    text = text.replace("_", " ")
    # Insert spaces before uppercase letters that are preceded by lowercase letters
    text = re.sub(r"(?<=[a-z])([A-Z])", r" \1", text)
    # Convert to lowercase
    return text.casefold()


def sanitize_instance_external_id(external_id: str) -> str:
    """Sanitize an instance external ID to be compatible with CDF requirements.

    Args:
        external_id: The external ID to sanitize.

    Returns:
        The sanitized external ID.
    """
    # CDF instance external IDs must be between 1 and 256 characters,
    if not external_id or external_id == "\x00":
        raise ToolkitValueError("External ID cannot be empty.")
    elif len(external_id) <= 256:
        return external_id
    hasher = hashlib.sha256()
    hasher.update(external_id.encode("utf-8"))
    hash_digest = hasher.hexdigest()[:8]
    sanitized_external_id = f"{external_id[:247]}_{hash_digest}"
    return sanitized_external_id
