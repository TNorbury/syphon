"""syphon.core.check.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import Callable, Optional

from ..hash import SplitResult

DEFAULT_FILE = ".sha256sums"


def check(
    cache_filepath: str,
    hash_filepath: Optional[str] = None,
    hash_line_split: Optional[Callable[[str], Optional[SplitResult]]] = None,
    verbose: bool = False,
) -> bool:
    """Verify the integrity of the built cache file.

    Args:
        cache_filepath: Absolute path to the target output file.
        hash_filepath: Absolute path to a file containing a SHA256 sum of the
            cache. If not given, then the default is calculated by joining the cache
            directory with `syphon.check.DEFAULT_FILE`.
        hash_line_split: A callable object that returns a `syphon.util.SplitResult`
            from a given line or None if the line is in an unexpected format. Returning
            None raises a MalformedLineError.
        verbose: Whether to print what is being done to the standard output.

    Returns:
        True if the cache file passed the integrity check, False otherwise.
    """
    import os

    from syphon.errors import MalformedLineError
    from syphon.hash import HashEntry, HashFile

    if hash_filepath is None:
        cachepath, cachefile = os.path.split(cache_filepath)
        hash_filepath = os.path.join(cachepath, DEFAULT_FILE)

    if not os.path.exists(hash_filepath):
        if verbose:
            print("No file exists @ {}".format(hash_filepath))
        return False

    expected_entry: Optional[HashEntry] = None
    actual_entry = HashEntry(cache_filepath)

    try:
        # Find the hash entry for the provided cache filepath.
        with HashFile(hash_filepath) as hashfile:
            hashfile.line_split = hash_line_split
            for entry in hashfile:
                if entry.filepath == actual_entry.filepath:
                    expected_entry = entry
                    break
    except OSError:
        if verbose:
            print("Error reading hash file @ {}".format(hash_filepath))
        return False
    except MalformedLineError as err:
        if verbose:
            print('Error parsing hash entry "{}"'.format(err.line))
        return False
    finally:
        del hashfile

    if expected_entry is None:
        if verbose:
            print(
                'No entry for file "{0}" found in "{1}"'.format(
                    cache_filepath, hash_filepath
                )
            )
        return False

    try:
        return expected_entry.hash == actual_entry.hash
    except OSError:
        if verbose:
            print("Error reading cache file @ {}".format(cache_filepath))
        return False