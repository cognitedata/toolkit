from unittest.mock import MagicMock

import pytest
from rich.console import Console

from cognite_toolkit._cdf_tk.utils.text import suffix_description


class TestSuffixDescription:
    @pytest.mark.parametrize(
        "description, expected_description, expect_warning",
        [
            (None, "<suffix>", False),
            ("", "<suffix>", False),
            ("Existing description", "Existing description <suffix>", False),
            ("Existing description <suffix>", "Existing description <suffix>", False),
            (
                "A very long description that exceeds the limit with more than one character",
                "A very long descrip...<suffix>",
                True,
            ),
        ],
    )
    def test_suffix_description(self, description: str | None, expected_description: str, expect_warning: True) -> None:
        console = MagicMock(spec=Console)
        limit = 30
        actual = suffix_description("<suffix>", description, limit, "identifier", "my_resource_type", console=console)

        assert actual == expected_description
        assert len(actual) <= limit
        if expect_warning:
            assert console.print.call_count == 1
            message = "".join(console.print.call_args[0])
            assert "Description is too long for my_resource_type 'identifier'." in message
