// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// AWS S3 "aws-chunked" content encoding (sigv4-streaming). Wire format:
//
//   <hex-size>[;chunk-signature=<sig>]\r\n <data> \r\n  ... 0[;chunk-signature=<sig>]\r\n \r\n
//
// Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html

const (
	S3StreamingPrefix = "STREAMING-"
	AwsChunkBufSize   = 4 * cos.KiB

	// A chunk size is a 64-bit value encoded in hex. 64 bits / 4 bits-per-hex-digit = 16 digits max.
	maxHexDigits = 16
)

// awsChunkedReader strips AWS chunked framing, returning only raw payload.
type awsChunkedReader struct {
	body       io.ReadCloser
	buf        []byte
	rpos, wpos int
	remaining  int
	started    bool
	done       bool
}

func newAwsChunkedReader(body io.ReadCloser) *awsChunkedReader {
	return &awsChunkedReader{body: body, buf: make([]byte, AwsChunkBufSize)}
}

// Read implements io.Reader. Each call either:
//   - parses the next chunk header to learn its data size (when remaining == 0), or
//   - copies chunk data into p (when remaining > 0).
func (r *awsChunkedReader) Read(p []byte) (int, error) {
	for !r.done {
		if r.remaining == 0 {
			size, err := r.nextChunk()
			if err != nil {
				return 0, err
			}
			if size == 0 {
				r.done = true
				break
			}
			r.remaining = size
			continue
		}
		limit := min(len(p), r.remaining)
		// first: drain any look-ahead bytes already sitting in buf
		if n := copy(p[:limit], r.buf[r.rpos:r.wpos]); n > 0 {
			r.rpos += n
			r.remaining -= n
			return n, nil
		}
		// buf empty: read directly from body into caller's buffer
		n, err := r.body.Read(p[:limit])
		r.remaining -= n
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, io.EOF
}

// nextChunk parses the next chunk header and returns its payload size.
//
// A chunk header looks like: "1e;chunk-signature=abc...\r\n"
// We need to extract "1e" (the hex size), ignore everything after ";", and
// stop at "\n". The header might not fit in a single buffer fill.
// That's why there are two loops:
//
//	outer loop: keeps the buffer filled — whenever the inner loop runs out of
//	            bytes, we read the next batch from body and resume scanning.
//	inner loop: scans through buf[rpos:wpos] one byte at a time, building up
//	            the hex size until it hits '\n' (end of header).
func (r *awsChunkedReader) nextChunk() (int, error) {
	// skip the `\r\n` terminator left over from the previous chunk's data
	// (the very first chunk has no preceding terminator, so we skip this)
	if r.started {
		if err := r.discardCRLF(); err != nil {
			return 0, fmt.Errorf("aws-chunked: CRLF: %w", err)
		}
	}
	r.started = true

	// parse the header
	var (
		size    = 0     // accumulate hex digits into `size`, stop at '\n'
		nDigits = 0     // number of hex digits parsed so far
		hexDone = false // flips to true once we hit ';' or '\r' (past the hex portion)
	)

	for {
		// scan through bytes that are already in the buffer
		for ; r.rpos < r.wpos; r.rpos++ {
			b := r.buf[r.rpos]

			if b == '\n' {
				r.rpos++ // consume the '\n' itself
				return size, nil
			}
			if hexDone || b == ';' || b == '\r' {
				hexDone = true // everything from here to '\n' is signature — skip
				continue
			}
			// still reading hex digits (e.g. '1', 'e')
			d, ok := cos.Unhex(b)
			if !ok {
				return 0, fmt.Errorf("aws-chunked: bad hex digit 0x%02x", b)
			}
			nDigits++
			if nDigits > maxHexDigits {
				return 0, fmt.Errorf("aws-chunked: chunk size exceeds %d hex digits", maxHexDigits)
			}
			size = size*16 + d
		}

		// ran out of buffered bytes before finding '\n' — refill and resume
		if err := r.refill(); err != nil {
			return 0, err
		}
	}
}

// discardCRLF skips and validates the 2-byte \r\n separator between chunk data and the next header.
func (r *awsChunkedReader) discardCRLF() error {
	expect := [2]byte{'\r', '\n'}
	for i := range 2 {
		if r.rpos >= r.wpos {
			if err := r.refill(); err != nil {
				return err
			}
		}
		if r.buf[r.rpos] != expect[i] {
			return fmt.Errorf("aws-chunked: expected 0x%02x, got 0x%02x", expect[i], r.buf[r.rpos])
		}
		r.rpos++
	}
	return nil
}

// refill resets the buffer cursors and reads the next batch from body.
// EOF is always unexpected here — a valid stream ends with a zero-length
// terminator chunk, so the caller never needs to refill past the end.
func (r *awsChunkedReader) refill() error {
	r.rpos, r.wpos = 0, 0
	n, err := r.body.Read(r.buf)
	r.wpos = n
	if n == 0 {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func (r *awsChunkedReader) Close() error {
	return r.body.Close()
}

// IsAwsChunked reports whether the request uses AWS chunked content encoding.
func IsAwsChunked(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(cos.S3HdrContentSHA256), S3StreamingPrefix)
}

// HandleAwsChunked wraps r.Body with a decoder that strips AWS chunked
// framing. Adjusts r.ContentLength and resets X-Amz-Content-Sha256.
func HandleAwsChunked(r *http.Request) {
	if decLen := r.Header.Get(cos.S3HdrDecodedContentLength); decLen != "" {
		if size, err := strconv.ParseInt(decLen, 10, 64); err == nil {
			r.ContentLength = size
			r.Header.Set(cos.HdrContentLength, decLen)
		}
	}
	r.Body = newAwsChunkedReader(r.Body)
	r.Header.Set(cos.S3HdrContentSHA256, cos.S3UnsignedPayload)
}
