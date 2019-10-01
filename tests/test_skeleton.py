# -*- coding: utf-8 -*-

import pytest
from postgres_pandas.skeleton import fib

__author__ = "Jan Therhaag"
__copyright__ = "Jan Therhaag"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
