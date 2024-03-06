#!/usr/bin/env python

import pytest


@pytest.mark.toolkit
def test_tool_globals(ToolGlobals):
    print(ToolGlobals)
    assert True is True
