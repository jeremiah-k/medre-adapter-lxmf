"""A parked protocol reader cannot hand its lock to SDK fork children.

The worker reads protocol frames from a background thread for its whole
life. The SDK forks worker subprocesses (LXMF stamp generation), and every
fork child starts by closing ``sys.stdin``. When the reader is parked on
``sys.stdin.buffer``, the child inherits the held buffer lock and
deadlocks before its own job runs. The protocol stream is therefore read
from a dup of file descriptor 0, leaving ``sys.stdin`` untouched.
"""

import io
import multiprocessing
import os
import subprocess
import sys
import textwrap

import pytest

from medre.adapters.lxmf.native_worker import _open_protocol_stdin

_FORK_AVAILABLE = "fork" in multiprocessing.get_all_start_methods()

_PROBE = """
import io
import multiprocessing
import os
import sys
import threading

from medre.adapters.lxmf.native_worker import _open_protocol_stdin

class SignaledRaw(io.RawIOBase):
    def __init__(self, raw):
        super().__init__()
        self.raw = raw
        self.entered = threading.Event()

    def readable(self):
        return True

    def readinto(self, buffer):
        # BufferedReader holds its own lock while calling this method.
        self.entered.set()
        return self.raw.readinto(buffer)


raw = SignaledRaw(
    _open_protocol_stdin() if sys.argv[1] == "dup" else sys.stdin.buffer.raw
)
stream = io.BufferedReader(raw)
if sys.argv[1] == "sysin":
    sys.stdin = io.TextIOWrapper(stream, encoding="utf-8")
parked = threading.Thread(target=stream.read, args=(4,), daemon=True)
parked.start()
if not raw.entered.wait(timeout=8):
    os._exit(5)

context = multiprocessing.get_context("fork")


def child():
    sys.stdin.close()


process = context.Process(target=child, daemon=True)
process.start()
process.join(timeout=6)
if process.is_alive():
    process.terminate()
    process.join()
    print("child-hung", flush=True)
    # Finalization would re-enter the same measured lock; exit without it.
    os._exit(4)
os._exit(process.exitcode if process.exitcode is not None else 0)
"""


def _probe(mode: str) -> int:
    """Fork a child beside a parked protocol reader and return its exit code.

    Stdin stays an unwritten pipe for the whole probe so the parked reader
    never observes EOF, mirroring the worker's lifetime read parking.
    """
    process = subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(_PROBE), mode],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        returncode = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise
    finally:
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
    return returncode


def test_protocol_stdin_is_a_raw_dup_disjoint_from_sys_stdin():
    """The protocol stream is a raw descriptor that is not fd 0 itself."""
    stream = _open_protocol_stdin()
    try:
        assert stream is not sys.stdin.buffer
        assert isinstance(stream, io.FileIO)
        assert stream.fileno() != 0
    finally:
        stream.close()


def test_protocol_stdin_reads_file_descriptor_zero():
    """Protocol frames written to fd 0 arrive on the dup'd stream."""
    saved = os.dup(0)
    read_fd, write_fd = os.pipe()
    try:
        os.dup2(read_fd, 0)
        os.close(read_fd)
        os.write(write_fd, b"frame")
        stream = _open_protocol_stdin()
        try:
            assert stream.read(5) == b"frame"
        finally:
            stream.close()
    finally:
        os.close(write_fd)
        os.dup2(saved, 0)
        os.close(saved)


def test_protocol_stdin_falls_back_to_sys_stdin_when_dup_unavailable(monkeypatch):
    """Environments without a dupable fd 0 keep the previous input stream."""

    def refuse(fd):
        raise OSError("dup unavailable")

    monkeypatch.setattr(os, "dup", refuse)
    assert _open_protocol_stdin() is sys.stdin.buffer


@pytest.mark.skipif(not _FORK_AVAILABLE, reason="requires POSIX fork")
def test_parked_protocol_reader_leaves_stdin_free_for_fork_children():
    """A fork child beside a parked dup'd reader exits instead of deadlocking."""
    assert _probe("dup") == 0


@pytest.mark.skipif(not _FORK_AVAILABLE, reason="requires POSIX fork")
def test_parked_stdin_buffer_reader_is_the_hazard_this_design_avoids():
    """The pre-fix shape deadlocks the child in _close_stdin, pinning the hazard."""
    assert _probe("sysin") == 4
