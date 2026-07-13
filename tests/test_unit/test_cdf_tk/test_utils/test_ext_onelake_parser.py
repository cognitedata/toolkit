import pytest

from cognite_toolkit._cdf_tk.utils.cdf import get_ext_onelake_source_ids


@pytest.mark.parametrize(
    "query, expected",
    [
        pytest.param(
            "select * from ext_onelake('fabric-prod', 'assets')",
            ["fabric-prod"],
            id="single_quote",
        ),
        pytest.param(
            'select * from EXT_ONELAKE("fabric-prod", "assets")',
            ["fabric-prod"],
            id="double_quote_case_insensitive",
        ),
        pytest.param(
            "select a from ext_onelake('source-a', 't1') join ext_onelake('source-b', 't2') on true",
            ["source-a", "source-b"],
            id="multiple_sources",
        ),
        pytest.param("select 1", [], id="no_ext_onelake"),
    ],
)
def test_get_ext_onelake_source_ids(query: str, expected: list[str]) -> None:
    assert get_ext_onelake_source_ids(query) == expected
