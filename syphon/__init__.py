# flake8: noqa
"""syphon.__init__.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from . import _version
from ._cmdparser import get_parser
from .archive import archive
from .build_ import build
from .check import check
from .init import init

__url__ = "https://github.com/tektronix/syphon"

__version__ = _version.get_versions()["version"]
del _version

__all__ = [
    "archive",
    "build",
    "check",
    "get_parser",
    "init",
    "__url__",
    "__version__",
]
