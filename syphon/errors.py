"""syphon.errors.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""


class MalformedLineError(BaseException):
    def __init__(self, line: str):
        super().__init__()
        self.line = line
