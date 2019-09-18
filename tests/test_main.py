"""tests.test_main.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import os
from typing import List

from _pytest.capture import CaptureFixture, CaptureResult
from py._path.local import LocalPath
from sortedcontainers import SortedDict

import syphon
import syphon.__main__
import syphon.schema

from . import get_data_path

SCHEMA = SortedDict({"0": "Species", "1": "PetalColor"})


def _archive_args(archive_dir_fixture: LocalPath) -> List[str]:
    return [
        "syphon",
        "archive",
        "-d",
        os.path.join(get_data_path(), "iris-part-*-of-6.csv"),
        "-m",
        os.path.join(get_data_path(), "iris-part-*-of-6.meta"),
        str(archive_dir_fixture),
    ]


def _build_args(
    archive_dir_fixture: LocalPath, cache_file_fixture: LocalPath
) -> List[str]:
    return ["syphon", "build", str(archive_dir_fixture), str(cache_file_fixture)]


def _init_args(archive_dir_fixture: LocalPath) -> List[str]:
    result: List[str] = ["syphon", "init", str(archive_dir_fixture)]
    result.extend(SCHEMA.values())
    return result


def test_main_help_argument_prints_full_help():
    # syphon.__main__:main is the entry point, so make sure it inherits sys.argv
    # (subprocess.Popen is used so main doesn't use our sys.argv).
    import subprocess

    proc = subprocess.Popen("python -m syphon --help", stdout=subprocess.PIPE)
    proc.wait()

    output: str
    output, _ = proc.communicate()

    assert proc.returncode == 0
    assert output.lower().startswith(b"usage:")
    assert output.find(bytes(syphon.__url__, "utf8")) != -1


def test_main_help_no_args_prints_usage():
    # syphon.__main__:main is the entry point, so make sure it inherits sys.argv
    # (subprocess.Popen is used so main doesn't use our sys.argv).
    import subprocess

    proc = subprocess.Popen("python -m syphon", stdout=subprocess.PIPE)
    proc.wait()

    output: str
    output, _ = proc.communicate()

    assert proc.returncode == 1
    assert output.lower().startswith(b"usage:")
    # Only the full help output contains the project URL.
    assert output.find(bytes(syphon.__url__, "utf8")) == -1


def test_main_archive(archive_dir: LocalPath):
    from glob import glob

    assert syphon.__main__.main(_init_args(archive_dir)) == 0

    archive_args: List[str] = _archive_args(archive_dir)

    returncode: int = syphon.__main__.main(archive_args)
    assert returncode == 0
    assert len(glob(os.path.join(archive_dir, "**"), recursive=True)) > 1


def test_main_build(archive_dir: LocalPath, cache_file: LocalPath):
    if os.path.exists(cache_file):
        os.remove(cache_file)

    assert syphon.__main__.main(_init_args(archive_dir)) == 0
    assert syphon.__main__.main(_archive_args(archive_dir)) == 0

    build_args: List[str] = _build_args(archive_dir, cache_file)

    returncode: int = syphon.__main__.main(build_args)
    assert returncode == 0
    assert os.path.exists(cache_file)
    assert cache_file.size() > 0


def test_main_init(archive_dir: LocalPath):
    init_args: List[str] = _init_args(archive_dir)

    returncode: int = syphon.__main__.main(init_args)
    assert returncode == 0

    actual_schema = syphon.schema.load(
        os.path.join(archive_dir, syphon.schema.DEFAULT_FILE)
    )
    assert actual_schema == SCHEMA


def test_main_version(capsys: CaptureFixture):
    arguments: List[str] = ["syphon", "--version"]

    returncode: int = syphon.__main__.main(arguments)
    assert returncode == 0

    output: CaptureResult = capsys.readouterr()
    assert output.out == "{}\n".format(syphon.__version__)
