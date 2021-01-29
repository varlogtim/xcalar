# flake8: noqa
# This file is bridging python APIs implemented in C++ to python interpreter
# land. This involves some trickery, so we disable the static analyzer.

import sys

_in_xpu = False
try:
    from _xpu_host import *
    _in_xpu = True
except ModuleNotFoundError:
    pass


def is_xpu():
    return _in_xpu


def is_app():
    return is_xpu() and has_running_app()
