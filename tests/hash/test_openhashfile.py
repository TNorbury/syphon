"""tests.hash.test_openhashfile.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os
from typing import List, Optional

from py._path.local import LocalPath

import syphon.hash
from _io import _IOBase

from .. import get_data_path, rand_string


def test_openhashfile_init(cache_file: LocalPath, hash_type: Optional[str]):
    if hash_type is None:
        hash_type = syphon.hash.DEFAULT_HASH_TYPE

    cache_file.write(rand_string())

    file_obj: _IOBase = cache_file.open("r+t")
    openhashfile = syphon.hash._OpenHashFile(file_obj, hash_type)
    try:
        assert file_obj.fileno() == openhashfile._file_obj.fileno()
        assert not openhashfile._file_obj.closed
        assert openhashfile.hash_type == hash_type
        assert openhashfile.line_split is None
    finally:
        file_obj.close()
        openhashfile._file_obj.close()


def test_openhashfile_close(cache_file: LocalPath):
    cache_file.write(rand_string())

    openhashfile = syphon.hash._OpenHashFile(cache_file.open("r+t"), "")

    openhashfile.close()
    try:
        assert openhashfile._file_obj.closed
    finally:
        openhashfile._file_obj.close()


def test_openhashfile_items_are_hashentries(tmpdir: LocalPath):
    target_hashfile: LocalPath = tmpdir.join("sha256sums")

    # Generate hashfile content.
    expected_entries: List[syphon.hash.HashEntry] = [
        syphon.hash.HashEntry(os.path.join(get_data_path(), "empty.csv")),
        syphon.hash.HashEntry(os.path.join(get_data_path(), "iris.csv")),
        syphon.hash.HashEntry(os.path.join(get_data_path(), "iris_plus.csv")),
    ]
    hashfile_content = "\n".join([str(e) for e in expected_entries])
    target_hashfile.write(hashfile_content)

    # Iterate through the hash file entries.
    with syphon.hash.HashFile(target_hashfile) as openfile:
        for actual in openfile:
            expected = expected_entries.pop(0)
            assert isinstance(actual, syphon.hash.HashEntry)
            assert str(expected) == str(actual)


def test_openhashfile_tell(cache_file: LocalPath):
    cache_file.write(rand_string())

    openhashfile = syphon.hash._OpenHashFile(cache_file.open("r+t"), "")
    assert openhashfile.tell() == 0
    assert openhashfile.tell() == openhashfile._file_obj.tell()

    line = openhashfile._file_obj.readline()
    assert openhashfile._file_obj.tell() == len(line)
