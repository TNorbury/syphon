"""tests.util.test_hashentry.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import hashlib
import os
from _hashlib import HASH
from typing import AnyStr, Optional, Union

import pytest
from py._path.local import LocalPath
from _pytest.fixtures import FixtureRequest

from syphon.util import DEFAULT_HASH, HashEntry, SplitResult

from .. import get_data_path


@pytest.fixture(params=["empty.csv", "iris.csv", "iris_plus.csv"])
def data_file(request: FixtureRequest) -> str:
    return request.param


@pytest.fixture(params=[True, False])
def binary_hash(request: FixtureRequest) -> bool:
    return request.param


@pytest.fixture(params=[None, hashlib.md5(), hashlib.sha1(), hashlib.sha512()])
def hash_type(request: FixtureRequest) -> str:
    return request.param


def _copy(source: Union[AnyStr, LocalPath], destination: Union[AnyStr, LocalPath]):
    if not isinstance(source, LocalPath):
        source = LocalPath(source)
    source.copy(destination)


def test_hashentry_init(data_file: str, binary_hash: bool, hash_type: Optional[HASH]):
    entry = HashEntry(data_file, binary=binary_hash, hash_obj=hash_type)
    assert entry._hash_cache == ""
    assert entry._hash_obj.name == (
        DEFAULT_HASH.name if hash_type is None else hash_type.name
    )
    assert entry._raw_entry == ""
    assert entry.binary == binary_hash
    assert entry.filepath == data_file


def test_hashentry_init_creates_new_hash_obj(hash_type: Optional[HASH]):
    if hash_type is None:
        hash_type = DEFAULT_HASH
    hash_type = hashlib.new(hash_type.name)

    hash_type.update(b"existing data")

    entry = HashEntry("datafile", hash_obj=hash_type)
    assert entry._hash_obj.digest() != hash_type.digest()


def test_hashentry_str(
    cache_file: LocalPath, data_file: str, binary_hash: bool, hash_type: Optional[HASH]
):
    target = LocalPath(os.path.join(get_data_path(), data_file))
    _copy(target, cache_file)
    assert os.path.exists(cache_file)

    entry = HashEntry(str(cache_file), binary=binary_hash, hash_obj=hash_type)

    expected_str = "{0} {1}{2}".format(
        entry._hash(), "*" if binary_hash else " ", str(cache_file)
    )
    # We've fed content to the hash object, so we have to reinitialize it.
    entry._hash_obj = hashlib.new(entry._hash_obj.name)

    assert expected_str == str(entry)


def test_hashentry_cached_after_hash(cache_file: LocalPath, data_file: str):
    target = LocalPath(os.path.join(get_data_path(), data_file))
    _copy(target, cache_file)
    assert os.path.exists(cache_file)

    entry = HashEntry(str(cache_file))
    assert not entry.cached

    entry.hash
    assert entry.cached


def test_hashentry_hash(
    cache_file: LocalPath, data_file: str, hash_type: Optional[HASH]
):
    target = LocalPath(os.path.join(get_data_path(), data_file))
    _copy(target, cache_file)
    assert os.path.exists(cache_file)

    entry = HashEntry(str(cache_file), hash_obj=hash_type)

    if hash_type is None:
        hash_type = DEFAULT_HASH
    hash_type = hashlib.new(hash_type.name)
    with open(cache_file, "r") as fd:
        hash_type.update(bytes(fd.read(), fd.encoding))
    expected_hash: str = hash_type.hexdigest()

    assert expected_hash == entry.hash


def test_hashentry_hasher_getter(hash_type: HASH):
    entry = HashEntry("datafile", hash_obj=hash_type)
    assert (
        entry.hasher.name == DEFAULT_HASH.name if hash_type is None else hash_type.name
    )

    entry._hash_obj.update(b"seed")
    assert entry._hash_obj.digest() == entry.hasher.digest()


def test_hashentry_hasher_setter(hash_type: HASH):
    if hash_type is None:
        hash_type = DEFAULT_HASH
    hash_type = hashlib.new(hash_type.name)

    hash_type.update(b"existing data")

    entry = HashEntry("datafile")
    assert entry.hasher.name == DEFAULT_HASH.name

    entry.hasher = hash_type
    assert entry.hasher.name == hash_type.name
    assert entry.hasher.digest() != hash_type.digest()


def test_hashentry_hasher_setter_raises_typeerror():
    entry = HashEntry("datafile")

    entry._hash_obj.update(b"seed")
    pre_hash = entry._hash_obj.digest()
    pre_hash_name = entry.hasher.name

    with pytest.raises(TypeError):
        entry.hasher = 0

    assert pre_hash == entry._hash_obj.digest()
    assert pre_hash_name == entry.hasher.name
