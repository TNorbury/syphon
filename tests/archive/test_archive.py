"""tests.archive.test_archive.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from pandas import DataFrame, concat, read_csv
from pandas.testing import assert_frame_equal
from py._path.local import LocalPath
from sortedcontainers import SortedDict, SortedList

import syphon
import syphon.schema

from .. import get_data_path


@pytest.fixture(
    params=[
        ("iris.csv", SortedDict({"0": "Name"})),
        ("iris_plus.csv", SortedDict({"0": "Species", "1": "PetalColor"})),
        (
            "auto-mpg.csv",
            SortedDict({"0": "model year", "1": "cylinders", "2": "origin"}),
        ),
    ]
)
def archive_params(request: FixtureRequest) -> Tuple[str, SortedDict]:
    return request.param


@pytest.fixture(
    params=[
        (
            "iris-part-1-of-6",
            "iris-part-1-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
        (
            "iris-part-2-of-6",
            "iris-part-2-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
        (
            "iris-part-3-of-6",
            "iris-part-3-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
        (
            "iris-part-4-of-6",
            "iris-part-4-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
        (
            "iris-part-5-of-6",
            "iris-part-5-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
        (
            "iris-part-6-of-6",
            "iris-part-6-of-6-combined.csv",
            SortedDict({"0": "Species", "1": "PetalColor"}),
        ),
    ]
)
def archive_meta_params(request: FixtureRequest) -> Tuple[str, str, SortedDict]:
    return request.param


def _get_expected_paths(
    path: str,
    schema: SortedDict,
    subset: DataFrame,
    filename: str,
    data: SortedList = SortedList(),
) -> SortedList:
    path_list = data.copy()

    this_schema = schema.copy()
    try:
        _, header = this_schema.popitem(index=0)
    except KeyError:
        path_list.add(os.path.join(path, filename))
        return path_list

    if header not in subset.columns:
        return path_list

    for value in subset.get(header).drop_duplicates().values:
        new_subset = subset.loc[subset.get(header) == value]
        value = value.lower().replace(" ", "_")
        if value[-1] == ".":
            value = value[:-1]
        path_list = _get_expected_paths(
            os.path.join(path, value), this_schema, new_subset, filename, data=path_list
        )
    return path_list


def test_archive(
    archive_params: Tuple[str, SortedDict], archive_dir: LocalPath, overwrite: bool
):
    filename: str
    schema: SortedDict
    filename, schema = archive_params

    datafile = os.path.join(get_data_path(), filename)
    schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

    syphon.init(schema, schemafile, overwrite)

    expected_df = DataFrame(read_csv(datafile, dtype=str))
    expected_df.sort_values(list(expected_df.columns), inplace=True)
    expected_df.reset_index(drop=True, inplace=True)

    expected_paths: SortedList = _get_expected_paths(
        archive_dir, schema, expected_df, filename
    )

    if overwrite:
        for e in expected_paths:
            os.makedirs(os.path.dirname(e), exist_ok=True)
            with open(e, mode="w") as fd:
                fd.write("content")

    syphon.archive(datafile, archive_dir, schemafile, overwrite=overwrite)
    assert not os.path.exists(os.path.join(get_data_path(), "#lock"))

    actual_frame = DataFrame()
    actual_paths = SortedList()
    for root, _, files in os.walk(archive_dir):
        for f in files:
            if ".csv" in f:
                filepath: str = os.path.join(root, f)
                actual_paths.add(filepath)
                actual_frame = concat(
                    [actual_frame, DataFrame(read_csv(filepath, dtype=str))]
                )

    actual_frame.sort_values(list(actual_frame.columns), inplace=True)
    actual_frame.reset_index(drop=True, inplace=True)

    assert expected_paths == actual_paths
    assert_frame_equal(expected_df, actual_frame)


def test_archive_metadata(
    archive_meta_params: Tuple[str, str, SortedDict],
    archive_dir: LocalPath,
    overwrite: bool,
):
    filename: str
    expectedfilename: str
    schema: SortedDict
    filename, expectedfilename, schema = archive_meta_params

    datafile = os.path.join(get_data_path(), filename) + ".csv"
    metafile = os.path.join(get_data_path(), filename) + ".meta"
    schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

    syphon.init(schema, schemafile, overwrite)

    expected_df = DataFrame(
        # Read our dedicated *-combined.csv file instead of the import target.
        read_csv(os.path.join(get_data_path(), expectedfilename), dtype=str)
    )
    expected_df.sort_values(list(expected_df.columns), inplace=True)
    expected_df.reset_index(drop=True, inplace=True)

    expected_paths: SortedList = _get_expected_paths(
        archive_dir, schema, expected_df, filename + ".csv"
    )

    if overwrite:
        for e in expected_paths:
            os.makedirs(os.path.dirname(e))
            with open(e, mode="w") as fd:
                fd.write("content")

    syphon.archive(datafile, archive_dir, schemafile, metafile, overwrite)
    assert not os.path.exists(os.path.join(get_data_path(), "#lock"))

    actual_df = DataFrame()
    actual_paths = SortedList()
    for root, _, files in os.walk(archive_dir):
        for f in files:
            if ".csv" in f:
                filepath: str = os.path.join(root, f)
                actual_paths.add(filepath)
                actual_df = concat(
                    [actual_df, DataFrame(read_csv(filepath, dtype=str))]
                )

    actual_df.sort_values(list(actual_df.columns), inplace=True)
    actual_df.reset_index(drop=True, inplace=True)

    assert expected_paths == actual_paths
    assert_frame_equal(expected_df, actual_df)


def test_archive_no_schema(
    archive_params: Tuple[str, SortedDict], archive_dir: LocalPath, overwrite: bool
):
    filename: str
    filename, _ = archive_params

    datafile = os.path.join(get_data_path(), filename)

    expected_df = DataFrame(read_csv(datafile, dtype=str))
    expected_df.sort_values(list(expected_df.columns), inplace=True)
    expected_df.reset_index(drop=True, inplace=True)

    expected_paths = SortedList([os.path.join(archive_dir, filename)])

    if overwrite:
        for e in expected_paths:
            path: LocalPath = archive_dir.new()
            path.mkdir(os.path.basename(os.path.dirname(e)))
            with open(e, mode="w") as fd:
                fd.write("content")

    syphon.archive(datafile, archive_dir, overwrite=overwrite)
    assert not os.path.exists(os.path.join(get_data_path(), "#lock"))

    actual_frame = DataFrame()
    actual_paths = SortedList()
    for root, _, files in os.walk(archive_dir):
        for f in files:
            if ".csv" in f:
                filepath: str = os.path.join(root, f)
                actual_paths.add(filepath)
                actual_frame = concat(
                    [actual_frame, DataFrame(read_csv(filepath, dtype=str))]
                )

    actual_frame.sort_values(list(actual_frame.columns), inplace=True)
    actual_frame.reset_index(drop=True, inplace=True)

    assert expected_paths == actual_paths
    assert_frame_equal(expected_df, actual_frame)


def test_archive_fileexistserror(
    archive_params: Tuple[str, SortedDict], archive_dir: LocalPath
):
    filename: str
    schema: SortedDict
    filename, schema = archive_params

    datafile = os.path.join(get_data_path(), filename)
    schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

    syphon.init(schema, schemafile, True)

    expected_df = DataFrame(read_csv(datafile, dtype=str))

    expected_paths: SortedList = _get_expected_paths(
        archive_dir, schema, expected_df, filename
    )

    for e in expected_paths:
        os.makedirs(os.path.dirname(e), exist_ok=True)
        with open(e, mode="w") as f:
            f.write("content")

    with pytest.raises(FileExistsError):
        syphon.archive(datafile, archive_dir, schemafile, overwrite=False)

    assert not os.path.exists(os.path.join(get_data_path(), "#lock"))
