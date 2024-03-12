#!/usr/bin/env python

import pytest

from cognite_toolkit._cdf_tk.utils import CDFToolConfig


@pytest.mark.toolkit
def test_tool_globals(read_write_group_token):
    ToolGlobals = CDFToolConfig(token=read_write_group_token, project="big-smoke-test")
    print(ToolGlobals)
    assert True
