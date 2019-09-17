"""tests.util.__init__.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import AnyStr, List, Union

from py._path.local import LocalPath


def _copy(source: Union[AnyStr, LocalPath], destination: Union[AnyStr, LocalPath]):
    if not isinstance(source, LocalPath):
        source = LocalPath(source)
    source.copy(destination)


# TODO
def rand_hashfile(source_glob: str, hashfile: str, *exclude_globs: List[str]):
    raise NotImplementedError()
