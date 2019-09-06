"""syphon.check.fileparse.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import Callable, Optional


class MalformedLineError(BaseException):
    def __init__(self, line: str):
        super().__init__()
        self.line = line


class _ChecksumExtractor(object):
    def __init__(self):
        super().__init__()
        self.filepath = ""

    def __call__(
        self,
        filepath: str,
        line_match: Optional[Callable[(str), bool]] = None,
        line_reduce: Optional[Callable[(str), Optional[str]]] = None,
    ) -> Optional[str]:
        """Parse the relevant checksum from the checksum file.

        Args:
            filepath: Absolute path to a file containing the checksum.
            line_match: A callable object that acts as a predicate on lines of the
                checksum file. The predicate returns True if and only if the given
                line contains the checksum of the cache file.
            line_reduce: A callable object that returns the checksum from a given
                line or None if the line is in an unexpected format. Returning None
                raises a MalformedLineError.

        Raises:
            MalformedLineError: If the `line_reduce` callable returns None.
            OSError: If there was an error reading the given file.
        """
        self.filepath = filepath
        with open(filepath, "r") as fd:
            for line in fd:
                # Call the default if no match callable is given.
                if (
                    self.default_line_match(line)
                    if line_match is None
                    else line_match(line)
                ):
                    # Call the default if no reduce callable is given.
                    result = (
                        self.default_line_reduce(line)
                        if line_reduce is None
                        else line_reduce(line)
                    )
                    if result is None:
                        raise MalformedLineError(line)
                    return result

    def default_line_match(self, line: str) -> bool:
        from os.path import basename

        return line.find(basename(self.filepath)) != -1

    def default_line_reduce(self, line: str) -> Optional[str]:
        from re import search

        # Split always returns a list, so index element 0 is safe.
        result: str = line.split(" ")[0]
        return None if search(r"^[a-f0-9]{63}$", result) is None else result


# Expose a callable class object.
get_checksum = _ChecksumExtractor()
