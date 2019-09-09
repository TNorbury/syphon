"""syphon.check.check.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import Callable, Optional


def check(
    cache_filepath: str,
    checksum_filepath: Optional[str] = None,
    checksum_line_match: Optional[Callable[[str], bool]] = None,
    checksum_line_reduce: Optional[Callable[[str], Optional[str]]] = None,
    verbose: bool = False,
) -> bool:
    """Verify the integrity of the built cache file.

    Args:
        cache_filepath: Absolute path to the target output file.
        checksum_filepath: Absolute path to a file containing a SHA256 sum of the
            cache. If not given, then the default is calculated by joining the cache
            directory with `syphon.check.DEFAULT_FILE`.
        checksum_line_match: A callable object that acts as a predicate on lines of the
                checksum file. The predicate returns True if and only if the given
                line contains the checksum of the cache file.
        checksum_line_reduce: A callable object that returns the checksum from a given
            line or None if the line is in an unexpected format.
        verbose: Whether to print what is being done to the standard output.

    Returns:
        True if the cache file passed the integrity check, False otherwise.
    """
    from hashlib import sha256
    from os.path import exists, join, split

    from . import DEFAULT_FILE
    from .fileparse import get_checksum, MalformedLineError

    cachepath, cachefile = split(cache_filepath)
    sums_filepath = (
        join(cachepath, DEFAULT_FILE)
        if checksum_filepath is None
        else checksum_filepath
    )

    if not exists(sums_filepath):
        if verbose:
            print("No file exists @ {}".format(sums_filepath))
        return False

    try:
        expected_checksum: str = get_checksum(
            sums_filepath, checksum_line_match, checksum_line_reduce
        )
    except OSError:
        if verbose:
            print("Error reading checksum file @ {}".format(sums_filepath))
        return False
    except MalformedLineError as err:
        if verbose:
            print('Error parsing checksum line "{}"'.format(err.line))
        return False

    hashobj = sha256()
    try:
        with open(cache_filepath, "rb") as fd:
            hashobj.update(fd.read())
    except OSError:
        if verbose:
            print("Error reading cache @ {}".format(cache_filepath))
        return False
    actual_checksum: str = hashobj.hexdigest()

    return expected_checksum == actual_checksum
