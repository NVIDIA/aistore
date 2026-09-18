#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock

from requests.exceptions import ChunkedEncodingError

from aistore.sdk.obj.obj_file.errors import (
    ObjectFileReaderMaxResumeError,
    ObjectFileReaderUnexpectedEOF,
)
from aistore.sdk.obj.obj_file.stream import ResumableStream
from tests.utils import cases

DATA = b"0123456789abcdef"


def _make_stream(streams, cached=None, max_resume=3, known_length=True):
    """Each stream specifies its end offset, chunk size, and terminal error."""
    provider = Mock()
    provider.client.path = "objects/bucket/object"
    provider.client.head.side_effect = [
        Mock(present=present) for present in (cached or [False] * (max_resume + 1))
    ]
    provider.expected_end_position = len(DATA) if known_length else None
    provider.offsets = []

    def create_iter(offset=0):
        end, chunk_size, error = streams[len(provider.offsets)]
        provider.offsets.append(offset)

        def iterator():
            for pos in range(offset, end, chunk_size):
                yield DATA[pos : min(pos + chunk_size, end)]
            if error:
                raise error

        return iterator()

    provider.create_iter.side_effect = create_iter
    return ResumableStream(provider, max_resume), provider


def _drain(stream):
    return b"".join(bytes(chunk) for chunk in stream)


class TestResumableStreamDelivery(unittest.TestCase):
    """The stream delivers every byte of the object exactly once."""

    def test_uninterrupted_stream_yields_chunks(self):
        stream, _ = _make_stream([(16, 6, None)])

        self.assertEqual([6, 6, 4], [len(chunk) for chunk in stream])

    def test_broken_stream_resumes_when_cached(self):
        stream, provider = _make_stream(
            [(8, 4, ChunkedEncodingError("interrupted")), (16, 4, None)],
            cached=[True],
        )

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 8], provider.offsets)

    def test_replay_spanning_several_restarts_delivers_each_byte_once(self):
        error = ChunkedEncodingError("interrupted")
        stream, provider = _make_stream(
            [(8, 4, error), (4, 2, error), (12, 3, error), (16, 5, None)]
        )

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 0, 0, 0], provider.offsets)
        self.assertEqual(len(DATA), stream.position)

    def test_restart_can_become_cached_mid_replay(self):
        error = ChunkedEncodingError("interrupted")
        stream, provider = _make_stream(
            [(8, 4, error), (4, 2, error), (16, 3, None)],
            cached=[False, True],
        )

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 0, 8], provider.offsets)


class TestResumableStreamShortRead(unittest.TestCase):
    """A stream that ends before the object does must be resumed, not reported as EOF."""

    def test_clean_short_eof_resumes(self):
        stream, provider = _make_stream([(8, 4, None), (16, 3, None)], cached=[True])

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 8], provider.offsets)

    def test_clean_short_eof_while_replaying_resumes_again(self):
        """A restarted stream that also ends early must be replaced in turn."""
        stream, provider = _make_stream([(8, 4, None), (4, 2, None), (16, 3, None)])

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 0, 0], provider.offsets)

    def test_unknown_length_eof_mid_replay_resumes(self):
        """A restarted stream that stops before the replay completes is still short."""
        stream, provider = _make_stream(
            [(8, 4, ChunkedEncodingError("interrupted")), (4, 2, None), (16, 3, None)],
            known_length=False,
        )

        self.assertEqual(DATA, _drain(stream))
        self.assertEqual([0, 0, 0], provider.offsets)


class TestResumableStreamBudget(unittest.TestCase):
    """Resumes are capped, and the cap is restored by a restart."""

    @cases(ChunkedEncodingError("interrupted"), None)
    def test_exhausted_budget_raises(self, error):
        """Both a broken stream and a clean short EOF consume the budget."""
        stream, provider = _make_stream(
            [(8, 4, error), (4, 2, error), (4, 2, error)], max_resume=2
        )

        with self.assertRaises(ObjectFileReaderMaxResumeError):
            _drain(stream)

        # The budget allows two replacement streams after the original.
        self.assertEqual([0, 0, 0], provider.offsets)

    def test_clean_short_eof_reports_unexpected_eof_as_the_cause(self):
        """A stream that ends early without raising still reports why it was resumed."""
        stream, _ = _make_stream([(8, 4, None)], max_resume=0)

        with self.assertRaises(ObjectFileReaderMaxResumeError) as context:
            _drain(stream)

        self.assertIsInstance(
            context.exception.original_error, ObjectFileReaderUnexpectedEOF
        )

    def test_restart_restores_budget_and_position(self):
        """A restart discards all progress made by earlier streams."""
        error = ChunkedEncodingError("interrupted")
        stream, provider = _make_stream(
            [(4, 4, error), (16, 4, None), (16, 4, None)], cached=[True]
        )

        # The second chunk reaches the error, so the stream resumes before the restart.
        next(stream)
        next(stream)
        self.assertEqual(1, stream.resumes)
        self.assertEqual(8, stream.position)

        stream.restart()

        self.assertEqual(0, stream.resumes)
        self.assertEqual(0, stream.position)
        self.assertEqual([0, 4, 0], provider.offsets)
        self.assertEqual(DATA, _drain(stream))
