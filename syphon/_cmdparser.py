"""syphon._cmdparser.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
import argparse


def get_parser() -> argparse.ArgumentParser:
    """Return `ArgumentParser` used to parse `syphon` arguments."""
    from . import __url__

    epilog_last_line = "Syphon home page: <{}>".format(__url__)

    # create parser with the given arguments
    # conflict_handler='resolve' -- allows parser to have keyword
    #   specific help
    # formatter_class=RawDescriptionHelpFormatter -- format descriptions
    #   before output
    parser = argparse.ArgumentParser(
        add_help=False,
        conflict_handler="resolve",
        description="A data storage and management engine.",
        epilog=epilog_last_line,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        prog=__package__,
    )
    # force overwrite
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="overwrite existing files",
        required=False,
    )
    # help
    parser.add_argument(
        "-h",
        "--help",
        action="store_true",
        default=False,
        help="display this help and exit",
        required=False,
    )
    # verbosity
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="explain what is being done",
        required=False,
    )
    # version information
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="output version information and exit",
        required=False,
    )

    # multiline string for the subcommand description.
    # %(prog)s -- replaced with the name of the program
    subparser_description = (
        "Additional subcommand help is available via\n"
        "      %(prog)s subcommand (-h|--help)"
    )
    # create a subparser group within the original parser
    subparsers = parser.add_subparsers(
        description=subparser_description, title="subcommands"
    )

    # archive command
    # create archive subcommand parser
    archive_parser = subparsers.add_parser(
        "archive",
        epilog=epilog_last_line,
        help="import files into the archive directory",
    )
    # optional, hidden argument that is true when using this subparser
    # but does not exist otherwise
    archive_parser.add_argument(
        "--archive",
        action="store_true",
        default=True,
        help=argparse.SUPPRESS,
        required=False,
    )
    archive_parser.add_argument(
        "archive_destination",
        help="directory where data is archived",
        metavar="destination",
    )
    archive_parser.add_argument(
        "-d", "--data", help="data file or glob pattern", required=True
    )
    archive_parser.add_argument(
        "-m",
        "--metadata",
        default=None,
        help="metadata file or glob pattern",
        required=False,
    )

    # build command
    # create build subcommand parser
    build_parser = subparsers.add_parser(
        "build", epilog=epilog_last_line, help="combine archives into a single file"
    )
    # optional, hidden argument that is true when using this subparser
    # but does not exist otherwise
    build_parser.add_argument(
        "--build",
        action="store_true",
        default=True,
        help=argparse.SUPPRESS,
        required=False,
    )
    build_parser.add_argument(
        "build_source", help="directory where data is stored", metavar="source"
    )
    build_parser.add_argument(
        "build_destination", help="filename of the output file", metavar="destination"
    )

    # check command
    # create check subcommand parser
    check_parser = subparsers.add_parser(
        "check", epilog=epilog_last_line, help="checks the integrity of a built file"
    )
    # optional, hidden argument that is true when using this subparser
    # but does not exist otherwise
    check_parser.add_argument(
        "--check",
        action="store_true",
        default=True,
        help=argparse.SUPPRESS,
        required=False,
    )
    check_parser.add_argument(
        "check_target", help="file output by the build command", metavar="target"
    )
    help = "an optional file whose lines are whitespace-delimited, checksum-file pairs"
    check_parser.add_argument(
        "check_source", default=None, help=help, metavar="source", nargs="?"
    )
    del help

    # init command
    # create init subcommand parser
    init_parser = subparsers.add_parser(
        "init",
        epilog=epilog_last_line,
        help="create an archive directory storage schema",
    )
    # optional, hidden argument that is true when using this subparser
    # but does not exist otherwise
    init_parser.add_argument(
        "--init",
        action="store_true",
        default=True,
        help=argparse.SUPPRESS,
        required=False,
    )
    init_parser.add_argument(
        "init_destination",
        help="directory where data is archived",
        metavar="destination",
    )
    init_parser.add_argument(
        "headers",
        metavar="header",
        help="column header(s) to use for the archive hierarchy",
        nargs="+",
    )

    return parser
