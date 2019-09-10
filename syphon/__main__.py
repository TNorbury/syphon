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
    from sortedcontainers import SortedDict

    from argparse import ArgumentParser, Namespace

    from . import __version__, schema
    from ._cmdparser import get_parser
    from .archive import archive
    from .build_ import build
    from .check import check
    from .init import init

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

    try:
        # Handle each subcommand.
        if getattr(parsed_args, "archive", False):
            archive_dirpath: str = os.path.abspath(parsed_args.archive_destination)
            schema_filepath: str = os.path.join(archive_dirpath, schema.DEFAULT_FILE)
            archive(
                parsed_args.data,
                archive_dirpath,
                schema_filepath if os.path.exists(schema_filepath) else None,
                parsed_args.meta,
                parsed_args.force,
                parsed_args.verbose,
            )
        elif getattr(parsed_args, "build", False):
            build(
                os.path.abspath(parsed_args.build_source),
                os.path.abspath(parsed_args.build_destination),
                parsed_args.force,
                parsed_args.verbose,
            )
        elif getattr(parsed_args, "check", False):
            checksum_file: Optional[str] = parsed_args.check_source
            return int(
                not check(
                    os.path.abspath(parsed_args.check_target),
                    checksum_file
                    if checksum_file is None
                    else os.path.abspath(checksum_file),
                    verbose=parsed_args.verbose,
                )
            )
        elif getattr(parsed_args, "init", False):
            schema = SortedDict()
            for (i, header) in zip(
                range(0, len(parsed_args.headers)), parsed_args.headers
            ):
                schema["%d" % i] = header

            init(
                schema,
                os.path.join(
                    os.path.abspath(parsed_args.init_destination), schema.DEFAULT_FILE
                ),
                parsed_args.force,
                parsed_args.verbose,
            )
    except Exception as err:
        print(str(err))
        return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(2)
