"""syphon.core.archive.filemap.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os

from sortedcontainers import SortedDict, SortedList


def filemap(data: SortedList, meta: SortedList) -> SortedDict:
    """Create a data-metadata file pair map.

    If there are more data files than metadata files (or vise versa),
    then each data file will match to all metadata files.

    If there are equal number data and metadata files, then try to match
    each data file with a metadata file that has the same name
    (excluding the extension). If there is not a match for every data
    file, then revert to the previous matching scheme.

    Args:
        data: Ordered list of absolute data file paths.
        meta: Ordered list of absolute metadata file paths.

    Returns:
        Dictionary sorted by key which indexes string lists.

        Keys are the absolute file path of a data file as a
        string. Values are a string list containing the absolute
        file path of metadata files associated with a data file.
    """
    result = SortedDict()

    if len(data) == len(meta):
        # Associate one data file to one metadata file.
        for datafile in data:
            dataname, _ = os.path.splitext(os.path.basename(datafile))
            for metafile in meta:
                metaname, _ = os.path.splitext(os.path.basename(metafile))
                if dataname == metaname:
                    result[datafile] = [metafile]

    if len(result) == 0:
        # Associate each data file to all metadata files.
        for datafile in data:
            result[datafile] = [metafile for metafile in meta]
        return result

    return result
