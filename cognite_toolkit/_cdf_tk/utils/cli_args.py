from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def parse_view_str(view_str: str) -> ViewId:
    """Parse a view string into a ViewId.

    Args:
        view_str (str): The view string to parse.

    Returns:
        ViewId: The parsed ViewId.

    Raises:
        ToolkitValueError: If the view string is not in a valid format.

    >>> parse_view_str("my_space:my_view/v1")
    ViewId(space='my_space', external_id='my_view', version='v1')
    """
    space_and_rest = view_str.split(":", 1)
    if len(space_and_rest) != 2:
        raise ToolkitValueError(
            f"Invalid view string format: '{view_str}'. Expected format 'space:externalId/version'."
        )
    space = space_and_rest[0]
    external_id_and_version = space_and_rest[1].rsplit("/", 1)
    if len(external_id_and_version) != 2:
        raise ToolkitValueError(
            f"Invalid view string format: '{view_str}'. Expected format 'space:externalId/version'."
        )
    external_id = external_id_and_version[0]
    version = external_id_and_version[1]
    return ViewId(space=space, external_id=external_id, version=version)
