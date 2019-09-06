"""syphon.check.__init__.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from .check import check

DEFAULT_FILE = ".sha256sums"

__all__ = ["check", "DEFAULT_FILE"]
