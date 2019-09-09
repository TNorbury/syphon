"""tests.init.test_init.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os
from json import loads

import pytest
from _pytest.fixtures import FixtureRequest
from py._path.local import LocalPath
from sortedcontainers import SortedDict

import syphon
import syphon.schema


@pytest.fixture(
    params=[
        {"0": "column1"},
        {"0": "column2", "1": "column4"},
        {"0": "column1", "1": "column3", "2": "column4"},
    ]
)
def init_schema_fixture(request: FixtureRequest) -> SortedDict:
    return SortedDict(request.param)


def test_init(archive_dir: LocalPath, init_schema_fixture: SortedDict, overwrite: bool):
    schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

    syphon.init(init_schema_fixture, schemafile, overwrite)

    with open(schemafile, "r") as f:
        actual = SortedDict(loads(f.read()))

    assert actual == init_schema_fixture


def test_init_fileexistserror(archive_dir: LocalPath, init_schema_fixture: SortedDict):
    schemafile = os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)

    with open(schemafile, mode="w") as f:
        f.write("content")

    with pytest.raises(FileExistsError):
        syphon.init(init_schema_fixture, schemafile, overwrite=False)
