"""syphon.build.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""


LINUX_HIDDEN_CHAR: str = "."


def build(
    archive_dir: str,
    cache_filepath: str,
    overwrite: bool = False,
    verbose: bool = False,
):
    """Combine all archived data files into a single file.

    Args:
        archive_dir: Absolute path to the data storage directory.
        cache_filepath: Absolute path to the target output file.
        overwrite: Whether an existing cache file should be replaced.
        verbose: Whether activities should be printed to the standard output.

    Raises:
        OSError: File operation error. Error type raised may be
            a subclass of OSError.
        FileExistsError: Cache file exists and overwrite is
            False.
    """
    import os
    from typing import Tuple

    from pandas import DataFrame, read_csv

    file_list = list()

    if os.path.exists(cache_filepath) and not overwrite:
        raise FileExistsError("Cache file already exists")

    for root, _, files in os.walk(archive_dir):
        for file in files:
            # skip linux-style hidden files
            if file[0] is not LINUX_HIDDEN_CHAR:
                file_list.append(os.path.join(root, file))

    cache = DataFrame()
    for file in file_list:
        if verbose:
            print("Build: from {0}".format(file))

        data = DataFrame(read_csv(file, dtype=str))

        if verbose:
            data_shape: Tuple[int, int] = data.shape
            cache_pre_shape: Tuple[int, int] = cache.shape

        cache = cache.append(data)

        if verbose:
            print(
                "Build: appending data {0} onto cache {1} => {2}".format(
                    data_shape, cache_pre_shape, cache.shape
                )
            )

        cache.reset_index(drop=True, inplace=True)

    cache.to_csv(cache_filepath, index=False)

    if verbose:
        print("Build: wrote {0}".format(cache_filepath))
