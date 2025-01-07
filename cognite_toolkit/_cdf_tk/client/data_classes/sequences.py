"""Subclassing the SequenceRows and SequenceRowsList classes from the Cognite SDK to add the as_write method.

This makes the subclasses compatible with the Toolkit Loader framework, which requires the as_write
method to be implemented.
"""

from cognite.client.data_classes._base import WriteableCogniteResource, WriteableCogniteResourceList
from cognite.client.data_classes.sequences import SequenceRows, SequenceRowsList


class ToolkitSequenceRows(WriteableCogniteResource["ToolkitSequenceRows"], SequenceRows):
    def as_write(self) -> "ToolkitSequenceRows":
        return self


class ToolkitSequenceRowsList(WriteableCogniteResourceList[ToolkitSequenceRows, ToolkitSequenceRows], SequenceRowsList):
    _RESOURCE = ToolkitSequenceRows

    def as_write(self) -> "ToolkitSequenceRowsList":
        return self
