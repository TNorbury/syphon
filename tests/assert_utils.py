"""tests.assert_utils.py

   Copyright Keithley Instruments, LLC.
   Licensed under MIT (https://github.com/tektronix/syphon/blob/master/LICENSE)

"""
from typing import List

from _pytest.capture import CaptureResult


def assert_captured_outerr(captured: CaptureResult, has_stdout: bool, has_stderr: bool):
    no_data_msg = "Expected data on {0}, but found nothing."
    data_msg = 'Unexpected data on {0}: "{1}"'

    if has_stdout:
        assert len(captured.out) > 0, no_data_msg.format("stdout")
    else:
        assert len(captured.out) == 0, data_msg.format("stdout", captured.out)

    if has_stderr:
        assert len(captured.err) > 0, no_data_msg.format("stderr")
    else:
        assert len(captured.err) == 0, data_msg.format("stderr", captured.err)


def assert_matches_outerr(
    captured: CaptureResult, match_out: List[str], match_err: List[str]
):
    match_msg = 'Cannot find "{0}" in "{1}" from {2}'

    for out in match_out:
        assert str(captured.out).find(out) != -1, match_msg.format(
            out, captured.out, "stdout"
        )

    for err in match_err:
        assert str(captured.err).find(err) != -1, match_msg.format(
            err, captured.err, "stderr"
        )
