"""syphon.check.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import Callable, Optional

from .util import SplitResult


DEFAULT_FILE = ".sha256sums"


def check(
    cache_filepath: str,
    checksum_filepath: Optional[str] = None,
    checksum_line_split: Optional[Callable[[str], Optional[SplitResult]]] = None,
    verbose: bool = False,
) -> bool:
    """Verify the integrity of the built cache file.

    Args:
        cache_filepath: Absolute path to the target output file.
        checksum_filepath: Absolute path to a file containing a SHA256 sum of the
            cache. If not given, then the default is calculated by joining the cache
            directory with `syphon.check.DEFAULT_FILE`.
        checksum_line_split: A callable object that returns a `syphon.util.SplitResult`
            from a given line or None if the line is in an unexpected format. Returning
            None raises a MalformedLineError.
        verbose: Whether to print what is being done to the standard output.

    Returns:
        True if the cache file passed the integrity check, False otherwise.
    """
    import os

    from . import errors, util

    if checksum_filepath is None:
        cachepath, cachefile = os.path.split(cache_filepath)
        checksum_filepath = os.path.join(cachepath, DEFAULT_FILE)

    if not os.path.exists(checksum_filepath):
        if verbose:
            print("No file exists @ {}".format(checksum_filepath))
        return False

    expected_entry: Optional[util.HashEntry] = None
    actual_entry = util.HashEntry(cache_filepath)

    try:
        with util.HashFile(checksum_filepath) as hf:
            for next_entry in hf.entries(checksum_line_split):
                if next_entry.filepath == actual_entry.filepath:
                    expected_entry = next_entry
                    break
    except OSError:
        if verbose:
            print("Error reading hash file @ {}".format(checksum_filepath))
        return False
    except errors.MalformedLineError as err:
        if verbose:
            print('Error parsing hash entry "{}"'.format(err.line))
        return False
    finally:
        del hf

    if expected_entry is None:
        if verbose:
            print(
                'No entry for file "{0}" found in "{1}"'.format(
                    cache_filepath, checksum_filepath
                )
            )
        return False

    return expected_entry.hash == actual_entry.hash
