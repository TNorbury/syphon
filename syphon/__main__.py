"""syphon.__main__.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import sys
from typing import List, Optional


def main(args: Optional[List[str]] = None) -> int:
    """Main entry point.

    Returns:
        int: An integer exit code. `0` for success or `1` for failure.
    """
    import os
    from glob import glob
    from sortedcontainers import SortedDict

    from argparse import ArgumentParser, Namespace

    from . import __version__, schema
    from ._cmdparser import get_parser
    from .core.archive.archive import archive
    from .core.archive.filemap import MappingBehavior
    from .core.build import build, LINUX_HIDDEN_CHAR
    from .core.check import check
    from .core.init import init

    if args is None:
        args = sys.argv

    parser: ArgumentParser = get_parser()

    if len(args) <= 1:
        parser.print_usage()
        return 1

    parsed_args: Namespace = parser.parse_args(args[1:])

    if parsed_args.help is True:
        parser.print_help()
        return 0

    if parsed_args.version is True:
        print(__version__)
        return 0

    # Handle each subcommand.
    if getattr(parsed_args, "archive", False):
        # Collect data and metadata files.
        data_files: List[str] = []
        meta_files: List[str] = []
        masks: List[str] = (
            [] if parsed_args.meta_mask is None else parsed_args.meta_mask
        )
        for source in parsed_args.archive_source:
            for filepath in glob(os.path.abspath(source)):
                # If a mask is found within the filepath, then it's a metadata file.
                # Otherwise, it's a data file.
                for mask in masks:
                    if filepath.find(mask) != -1:
                        meta_files.append(filepath)
                        break
                else:
                    data_files.append(filepath)

        # Resolve the filemapping behavior.
        if getattr(parsed_args, "one_to_many", False):
            behavior = MappingBehavior.ONE_TO_MANY
        else:
            behavior = MappingBehavior.ONE_TO_ONE

        archive_dirpath: str = os.path.abspath(parsed_args.archive_destination)
        schema_filepath: str = os.path.join(archive_dirpath, schema.DEFAULT_FILE)
        return int(
            not archive(
                archive_dirpath,
                data_files,
                meta_files,
                filemap_behavior=behavior,
                schema_filepath=schema_filepath
                if os.path.exists(schema_filepath)
                else None,
                overwrite=parsed_args.force,
                verbose=parsed_args.verbose,
            )
        )
    elif getattr(parsed_args, "build", False):
        file_list: List[str] = list()
        for root, _, files in os.walk(os.path.abspath(parsed_args.build_source)):
            for file in files:
                # skip linux-style hidden files
                if not file.startswith(LINUX_HIDDEN_CHAR):
                    file_list.append(os.path.join(root, file))
        build(
            os.path.abspath(parsed_args.build_destination),
            *file_list,
            hash_filepath=(
                None
                if parsed_args.hashfile is None
                else os.path.abspath(parsed_args.hashfile)
            ),
            overwrite=parsed_args.force,
            verbose=parsed_args.verbose,
        )
    elif getattr(parsed_args, "check", False):
        return int(
            not check(
                os.path.abspath(parsed_args.check_target),
                hash_filepath=(
                    None
                    if parsed_args.hashfile is None
                    else os.path.abspath(parsed_args.hashfile)
                ),
                verbose=parsed_args.verbose,
            )
        )
    elif getattr(parsed_args, "init", False):
        new_schema = SortedDict()
        for (i, header) in zip(range(0, len(parsed_args.headers)), parsed_args.headers):
            new_schema.update(**{"%d" % i: header})

        init(
            new_schema,
            os.path.join(
                os.path.abspath(parsed_args.init_destination), schema.DEFAULT_FILE
            ),
            overwrite=parsed_args.force,
            verbose=parsed_args.verbose,
        )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
