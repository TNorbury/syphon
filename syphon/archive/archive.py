"""syphon.archive.archive.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from os.path import basename, dirname
from typing import List, Optional

from pandas import DataFrame, Series, concat, read_csv
from sortedcontainers import SortedDict, SortedList

from ._lockmanager import LockManager


class InconsistentMetadataError(BaseException):
    def __init__(self, column: str):
        super().__init__()
        self.column = column


def _merge_metafiles(
    filemap: SortedDict, datafile: str, data_rows: int
) -> Optional[DataFrame]:
    # merge all metadata files into a single DataFrame
    meta_frame = DataFrame()
    for metafile in filemap[datafile]:
        new_frame = DataFrame(read_csv(metafile, dtype=str))

        new_frame.dropna(axis=1, how="all", inplace=True)
        for header in list(new_frame.columns.values):
            # complain if there's more than one value in a column
            if len(new_frame[header].drop_duplicates().values) > 1:
                raise InconsistentMetadataError(header)

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
    overwrite: bool,
    verbose: bool,
):
    from os import makedirs
    from os.path import exists, join

    from ..schema import resolve_path

    datafilename = basename(datafile)

    for data in filtered_data:
        path: Optional[str] = None
        path = resolve_path(archive, schema, data)

        target_filename: str = join(
            path, datafilename
        ) if path is not None else datafilename

        if exists(target_filename) and not overwrite:
            raise FileExistsError(
                "Archive error: file already exists @ " "{}".format(target_filename)
            )

        makedirs(path, exist_ok=True)
        data.to_csv(target_filename, index=False)

        if verbose:
            print("Archive: wrote {0}".format(target_filename))


def _archive(
    archive_dir: str,
    schema: SortedDict,
    data_list: SortedList,
    meta_list: SortedList,
    overwrite: bool,
    verbose: bool,
):
    from pandas.errors import EmptyDataError

    from ..schema import check_columns

    from . import datafilter
    from . import file_map

    fmap: SortedDict = file_map(data_list, meta_list)

    for datafile in fmap:
        try:
            data_frame = DataFrame(read_csv(datafile, dtype=str))
        except EmptyDataError:
            # trigger the empty check below
            data_frame = DataFrame()

        if data_frame.empty:
            if verbose:
                print("Skipping empty data file @ {}".format(datafile))
            continue

        # remove empty columns
        data_frame.dropna(axis=1, how="all", inplace=True)

        try:
            meta_frame: Optional[DataFrame] = _merge_metafiles(
                fmap, datafile, data_frame.shape[0]
            )
        except InconsistentMetadataError as err:
            raise ValueError(
                "More than one value exists under the {} column.".format(err.column)
            )

        if meta_frame is not None:
            data_frame = concat([data_frame, meta_frame], axis=1)

        check_columns(schema, data_frame)

        filtered_data: List[DataFrame] = datafilter(schema, data_frame)

        if len(filtered_data) == 0:
            filtered_data = [data_frame]

        _write_filtered_data(
            archive_dir, schema, filtered_data, datafile, overwrite, verbose
        )


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
        FileNotFoundError: A given glob pattern failed to match any files.
        FileExistsError: An archive file already exists with the same filepath.
        IndexError: Schema value is not a column header of a given DataFrame.
        OSError: File operation error. Error type raised may be a subclass of OSError.
        ValueError: More than one unique metadata value exists under a column header.
        Exception: Any error raised by pandas.read_csv.
    """
    from glob import glob

    from ..schema import load

    lock_manager = LockManager()
    lock_list: List[str] = list()

    schema: SortedDict = SortedDict() if schema_filepath is None else load(
        schema_filepath
    )

    # add '#lock' file to all data directories
    data_list: SortedList = SortedList(glob(data_glob))
    if len(data_list) == 0:
        lock_manager.release_all()
        raise FileNotFoundError('No data files matching "{}"'.format(data_glob))
    lock_list.append(lock_manager.lock(dirname(data_list[0])))

    # add '#lock' file to all metadata directories
    meta_list: SortedList = SortedList()
    if meta_glob is not None:
        meta_list = SortedList(glob(meta_glob))
        if len(meta_list) == 0:
            lock_manager.release_all()
            raise FileNotFoundError('No metadata files matching "{}"'.format(data_glob))
        lock_list.append(lock_manager.lock(dirname(meta_list[0])))

    try:
        _archive(archive_dir, schema, data_list, meta_list, overwrite, verbose)
    except FileExistsError:
        raise
    except IndexError:
        raise
    except OSError:
        raise
    except ValueError:
        raise
    except Exception:
        raise
    finally:
        lock_manager.release_all()
