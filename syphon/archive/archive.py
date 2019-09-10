"""syphon.archive.archive.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from os.path import basename, dirname
from typing import List, Optional

from pandas import DataFrame, Series, concat, read_csv
from pandas.errors import ParserError
from sortedcontainers import SortedDict

from ._lockmanager import LockManager


def _merge_metafiles(
    filemap: SortedDict, datafile: str, data_rows: int, lockman: LockManager
) -> Optional[DataFrame]:
    # merge all metadata files into a single DataFrame
    meta_frame = DataFrame()
    for metafile in filemap[datafile]:
        try:
            new_frame = DataFrame(read_csv(metafile, dtype=str))
        except ParserError:
            lockman.release_all()
            raise

        new_frame.dropna(axis=1, how="all", inplace=True)
        for header in list(new_frame.columns.values):
            # complain if there's more than one value in a column
            if len(new_frame[header].drop_duplicates().values) > 1:
                lockman.release_all()
                raise ValueError(
                    "More than one value exists under the {} column.".format(header)
                )

            if len(new_frame[header]) is data_rows:
                meta_frame = concat([meta_frame, new_frame[header]], axis=1)
            else:
                meta_value = new_frame[header].iloc[0]
                series = Series([meta_value] * data_rows, name=header)
                meta_frame = concat([meta_frame, series], axis=1)

    return None if meta_frame.empty else meta_frame


def _write_filtered_data(
    archive: str,
    schema: SortedDict,
    filtered_data: List[DataFrame],
    datafile: str,
    lockman: LockManager,
    overwrite: bool,
    verbose: bool,
):
    from os import makedirs
    from os.path import exists, join

    from syphon.schema import resolve_path

    datafilename = basename(datafile)

    for data in filtered_data:
        path: Optional[str] = None
        try:
            path = resolve_path(archive, schema, data)
        except IndexError:
            lockman.release_all()
            raise
        except ValueError:
            lockman.release_all()
            raise

        target_filename: str = join(
            path, datafilename
        ) if path is not None else datafilename

        if exists(target_filename) and not overwrite:
            lockman.release_all()
            raise FileExistsError(
                "Archive error: file already exists @ " "{}".format(target_filename)
            )

        try:
            makedirs(path, exist_ok=True)
            data.to_csv(target_filename, index=False)
        except OSError:
            lockman.release_all()
            raise

        if verbose:
            print("Archive: wrote {0}".format(target_filename))


def archive(
    data_glob: str,
    archive_dir: str,
    schema_filepath: Optional[str] = None,
    meta_glob: Optional[str] = None,
    overwrite: bool = False,
    verbose: bool = False,
):
    """Store the files matching the given glob pattern.

    Args:
        data_glob: Glob pattern matching one or more data files.
        archive_dir: Absolute path to the data storage directory.
        schema_filepath: Absolute path to a JSON file containing a storage schema.
        meta_glob: Glob pattern matching one or more metadata files.
        overwrite: Whether existing files should be overwritten during archival.
        verbose: Whether activities should be printed to the standard output.

    Raises:
        FileExistsError: An archive file already exists with
            the same filepath.
        IndexError: Schema value is not a column header of a
            given DataFrame.
        OSError: File operation error. Error type raised may be
            a subclass of OSError.
        ParserError: Error raised by pandas.read_csv.
        ValueError: More than one unique metadata value exists
            under a column header.
    """
    from glob import glob

    from pandas.errors import EmptyDataError
    from sortedcontainers import SortedList
    from syphon.schema import check_columns, load

    from . import datafilter
    from . import file_map

    lock_manager = LockManager()
    lock_list: List[str] = list()

    schema: SortedDict = SortedDict() if schema_filepath is None else load(
        schema_filepath
    )

    # add '#lock' file to all data directories
    data_list: SortedList = SortedList(glob(data_glob))
    lock_list.append(lock_manager.lock(dirname(data_list[0])))

    # add '#lock' file to all metadata directories
    meta_list: SortedList = SortedList()
    if meta_glob is not None:
        meta_list = SortedList(glob(meta_glob))
        lock_list.append(lock_manager.lock(dirname(meta_list[0])))

    fmap: SortedDict = file_map(data_list, meta_list)

    for datafile in fmap:
        try:
            data_frame = DataFrame(read_csv(datafile, dtype=str))
        except EmptyDataError:
            # trigger the empty check below
            data_frame = DataFrame()
        except ParserError:
            lock_manager.release_all()
            raise

        if data_frame.empty:
            print("Skipping empty data file @ {}".format(datafile))
            continue

        # remove empty columns
        data_frame.dropna(axis=1, how="all", inplace=True)

        meta_frame: Optional[DataFrame] = _merge_metafiles(
            fmap, datafile, data_frame.shape[0], lock_manager
        )

        if meta_frame is not None:
            data_frame = concat([data_frame, meta_frame], axis=1)

        try:
            check_columns(schema, data_frame)
        except IndexError:
            lock_manager.release_all()
            raise

        filtered_data: List[DataFrame] = datafilter(schema, data_frame)

        if len(filtered_data) == 0:
            filtered_data = [data_frame]

        _write_filtered_data(
            archive_dir,
            schema,
            filtered_data,
            datafile,
            lock_manager,
            overwrite,
            verbose,
        )

    lock_manager.release_all()
