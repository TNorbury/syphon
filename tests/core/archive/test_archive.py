"""tests.core.archive.test_archive.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os
from typing import Tuple

import pytest
from _pytest.capture import CaptureFixture
from _pytest.fixtures import FixtureRequest
from pandas import DataFrame, concat, read_csv
from pandas.testing import assert_frame_equal
from py._path.local import LocalPath
from sortedcontainers import SortedDict, SortedList

import syphon
import syphon.schema

from ... import get_data_path, rand_string
from ...assert_utils import assert_captured_outerr


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


class TestArchive(object):
    @staticmethod
    def test_empty_datafile(
        capsys: CaptureFixture, archive_dir: LocalPath, verbose: bool
    ):
        datafile = os.path.join(get_data_path(), "empty.csv")

        assert not syphon.archive(archive_dir, [datafile], verbose=verbose)
        assert_captured_outerr(capsys.readouterr(), verbose, False)
        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_no_datafiles(
        capsys: CaptureFixture, archive_dir: LocalPath, verbose: bool
    ):
        assert not syphon.archive(archive_dir, [], verbose=verbose)
        assert_captured_outerr(capsys.readouterr(), verbose, False)

    @staticmethod
    def test_without_metadata_with_schema(
        capsys: CaptureFixture,
        archive_params: Tuple[str, SortedDict],
        archive_dir: LocalPath,
        overwrite: bool,
        verbose: bool,
    ):
        filename: str
        schema: SortedDict
        filename, schema = archive_params

        datafile = os.path.join(get_data_path(), filename)
        schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

        syphon.init(schema, schemafile)

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
                    fd.write(rand_string())

        assert syphon.archive(
            archive_dir,
            [datafile],
            schema_filepath=schemafile,
            overwrite=overwrite,
            verbose=verbose,
        )
        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

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
        assert_captured_outerr(capsys.readouterr(), verbose, False)

    @staticmethod
    def test_without_metadata_without_schema(
        capsys: CaptureFixture,
        archive_params: Tuple[str, SortedDict],
        archive_dir: LocalPath,
        overwrite: bool,
        verbose: bool,
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
                    fd.write(rand_string())

        assert syphon.archive(
            archive_dir, [datafile], overwrite=overwrite, verbose=verbose
        )
        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

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
        assert_captured_outerr(capsys.readouterr(), verbose, False)

    @staticmethod
    def test_with_metadata_with_schema(
        capsys: CaptureFixture,
        archive_meta_params: Tuple[str, str, SortedDict],
        archive_dir: LocalPath,
        overwrite: bool,
        verbose: bool,
    ):
        filename: str
        expectedfilename: str
        schema: SortedDict
        filename, expectedfilename, schema = archive_meta_params

        datafile = os.path.join(get_data_path(), filename + ".csv")
        metafile = os.path.join(get_data_path(), filename + ".meta")
        schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

        syphon.init(schema, schemafile)

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
                os.makedirs(os.path.dirname(e), exist_ok=True)
                with open(e, mode="w") as fd:
                    fd.write(rand_string())

        assert syphon.archive(
            archive_dir,
            [datafile],
            meta_files=[metafile],
            schema_filepath=schemafile,
            overwrite=overwrite,
            verbose=verbose,
        )
        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

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

    @staticmethod
    def test_with_metadata_without_schema(
        capsys: CaptureFixture,
        archive_meta_params: Tuple[str, str, SortedDict],
        archive_dir: LocalPath,
        overwrite: bool,
        verbose: bool,
    ):
        filename: str
        expectedfilename: str
        filename, expectedfilename, _ = archive_meta_params

        datafile = os.path.join(get_data_path(), filename + ".csv")
        metafile = os.path.join(get_data_path(), filename + ".meta")

        expected_df = DataFrame(
            # Read our dedicated *-combined.csv file instead of the import target.
            read_csv(os.path.join(get_data_path(), expectedfilename), dtype=str)
        )
        expected_df.sort_values(list(expected_df.columns), inplace=True)
        expected_df.reset_index(drop=True, inplace=True)

        expected_paths: SortedList = _get_expected_paths(
            archive_dir, SortedDict(), expected_df, filename + ".csv"
        )

        if overwrite:
            for e in expected_paths:
                os.makedirs(os.path.dirname(e), exist_ok=True)
                with open(e, mode="w") as fd:
                    fd.write(rand_string())

        assert syphon.archive(
            archive_dir,
            [datafile],
            meta_files=[metafile],
            overwrite=overwrite,
            verbose=verbose,
        )
        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

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

    @staticmethod
    def test_raises_fileexistserror_on_existing_archive_file(
        archive_params: Tuple[str, SortedDict], archive_dir: LocalPath
    ):
        filename: str
        schema: SortedDict
        filename, schema = archive_params

        datafile = os.path.join(get_data_path(), filename)
        schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

        syphon.init(schema, schemafile)

        expected_df = DataFrame(read_csv(datafile, dtype=str))

        expected_paths: SortedList = _get_expected_paths(
            archive_dir, schema, expected_df, filename
        )

        for e in expected_paths:
            os.makedirs(os.path.dirname(e), exist_ok=True)
            with open(e, mode="w") as f:
                f.write(rand_string())

        with pytest.raises(FileExistsError, match=os.path.basename(datafile)):
            syphon.archive(
                archive_dir, [datafile], schema_filepath=schemafile, overwrite=False
            )

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_raises_filenotfounderror_when_data_cannot_be_found(archive_dir: LocalPath):
        datafile = os.path.join(get_data_path(), "nonexistantfile.csv")

        with pytest.raises(FileNotFoundError, match="data file"):
            syphon.archive(archive_dir, [datafile])

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_raises_filenotfounderror_when_metadata_cannot_be_found(
        archive_params: Tuple[str, SortedDict], archive_dir: LocalPath
    ):
        filename: str
        schema: SortedDict
        filename, schema = archive_params

        datafile = os.path.join(get_data_path(), filename)
        metafile = os.path.join(get_data_path(), "nonexistantfile.meta")

        with pytest.raises(FileNotFoundError, match="metadata file"):
            syphon.archive(archive_dir, [datafile], meta_files=[metafile])

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_raises_filenotfounderror_when_schema_cannot_be_found(
        archive_params: Tuple[str, SortedDict], archive_dir: LocalPath
    ):
        filename: str
        schema: SortedDict
        filename, schema = archive_params

        datafile = os.path.join(get_data_path(), filename)

        with pytest.raises(FileNotFoundError, match="schema file"):
            syphon.archive(archive_dir, [datafile], schema_filepath=rand_string())

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_raises_indexerror_when_a_schema_column_does_not_exist(
        archive_meta_params: Tuple[str, str, SortedDict], archive_dir: LocalPath
    ):
        bad_column = "non_existent_column"

        filename: str
        expectedfilename: str
        schema: SortedDict
        filename, expectedfilename, schema = archive_meta_params

        # Add a bad column.
        local_schema = schema.copy()
        local_schema["%d" % len(local_schema)] = bad_column

        datafile = os.path.join(get_data_path(), filename + ".csv")
        metafile = os.path.join(get_data_path(), filename + ".meta")
        schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

        syphon.init(local_schema, schemafile)

        with pytest.raises(IndexError, match=bad_column):
            syphon.archive(
                archive_dir,
                [datafile],
                meta_files=[metafile],
                schema_filepath=schemafile,
                overwrite=True,
            )

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))

    @staticmethod
    def test_raises_valueerror_when_metadata_is_inconsistent(
        archive_meta_params: Tuple[str, str, SortedDict],
        archive_dir: LocalPath,
        import_dir: LocalPath,
    ):
        filename: str
        expectedfilename: str
        schema: SortedDict
        filename, expectedfilename, schema = archive_meta_params

        datafile = os.path.join(get_data_path(), filename + ".csv")
        bad_metafile = LocalPath(
            os.path.join(get_data_path(), filename + "-inconsistent.meta")
        )
        metafile = import_dir.join(filename + ".meta")
        bad_metafile.copy(metafile)
        schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

        syphon.init(schema, schemafile)

        # Find the column that will be in the message.
        metaframe = DataFrame(read_csv(metafile, dtype=str))
        for column in metaframe.columns:
            if len(metaframe[column].drop_duplicates().values) > 1:
                break
        del metaframe

        with pytest.raises(ValueError, match=column):
            syphon.archive(
                archive_dir,
                [datafile],
                meta_files=[metafile],
                schema_filepath=schemafile,
                overwrite=True,
            )

        assert not os.path.exists(os.path.join(os.path.dirname(datafile), "#lock"))
