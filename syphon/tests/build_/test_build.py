"""syphon.tests.build_.test_build.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os

import pytest
from pandas import DataFrame, read_csv
from pandas.testing import assert_frame_equal
from sortedcontainers import SortedDict
from syphon import Context
from syphon.archive import archive
from syphon.init import init
from syphon.build_ import build

from .. import get_data_path


class TestBuild(object):
    @staticmethod
    def _delete_cache(file: str):
        try:
            os.remove(file)
        except OSError:
            raise

    def test_build_iris(self, archive_dir, cache_file, overwrite):
        try:
            TestBuild._delete_cache(str(cache_file))
        except FileNotFoundError:
            pass
        except OSError:
            raise

        context = Context()
        context.archive = str(archive_dir)
        context.cache = str(cache_file)
        context.data = os.path.join(get_data_path(), 'iris.csv')
        context.overwrite = overwrite
        context.schema = SortedDict({'0': 'Name'})

        init(context)
        archive(context)
        assert not os.path.exists(os.path.join(get_data_path(), '#lock'))

        expected_frame = DataFrame(read_csv(context.data, dtype=str))
        expected_frame.sort_values('SepalLength', inplace=True)
        expected_frame.reset_index(drop=True, inplace=True)

        if context.overwrite:
            with open(context.cache, mode='w') as f:
                f.write('content')

        build(context)

        actual_frame = DataFrame(read_csv(context.cache, dtype=str))
        actual_frame.sort_values('SepalLength', inplace=True)
        actual_frame.reset_index(drop=True, inplace=True)

        assert_frame_equal(expected_frame, actual_frame)

    def test_build_iris_no_schema(self, archive_dir, cache_file, overwrite):
        try:
            TestBuild._delete_cache(str(cache_file))
        except FileNotFoundError:
            pass
        except OSError:
            raise

        context = Context()
        context.archive = str(archive_dir)
        context.cache = str(cache_file)
        context.data = os.path.join(get_data_path(), 'iris.csv')
        context.overwrite = overwrite
        context.schema = SortedDict()

        archive(context)
        assert not os.path.exists(os.path.join(get_data_path(), '#lock'))

        expected_frame = DataFrame(read_csv(context.data, dtype=str))
        expected_frame.sort_values('SepalLength', inplace=True)
        expected_frame.reset_index(drop=True, inplace=True)

        if context.overwrite:
            with open(context.cache, mode='w') as f:
                f.write('content')

        build(context)

        actual_frame = DataFrame(read_csv(context.cache, dtype=str))
        actual_frame.sort_values('SepalLength', inplace=True)
        actual_frame.reset_index(drop=True, inplace=True)

        assert_frame_equal(expected_frame, actual_frame)

    def test_build_fileexistserror(self, archive_dir, cache_file):
        try:
            TestBuild._delete_cache(str(cache_file))
        except FileNotFoundError:
            pass
        except OSError:
            raise

        context = Context()
        context.archive = str(archive_dir)
        context.cache = str(cache_file)
        context.data = os.path.join(get_data_path(), 'iris.csv')
        context.overwrite = False
        context.schema = SortedDict()

        archive(context)
        assert not os.path.exists(os.path.join(get_data_path(), '#lock'))

        with open(context.cache, mode='w') as f:
            f.write('content')

        with pytest.raises(FileExistsError):
            build(context)
