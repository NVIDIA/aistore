// Package s3_test provides tests for the Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestAwsChunkedSingleChunk(t *testing.T) {
	// 0x1e = 30 bytes
	body := "1e;chunk-signature=abc123\r\n" +
		"this is a file with some text\n" +
		"\r\n" +
		"0;chunk-signature=def456\r\n" +
		"\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "30")
	tassert.Fatalf(t, s3.IsAwsChunked(r), "expected IsAwsChunked true")

	s3.HandleAwsChunked(r)
	tassert.Fatalf(t, r.ContentLength == 30, "expected ContentLength 30, got %d", r.ContentLength)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "this is a file with some text\n",
		"expected decoded payload, got %q", string(data))
}

func TestAwsChunkedMultipleChunks(t *testing.T) {
	body := "5;chunk-signature=sig1\r\n" +
		"Hello" +
		"\r\n" +
		"6;chunk-signature=sig2\r\n" +
		" World" +
		"\r\n" +
		"0;chunk-signature=sig3\r\n" +
		"\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "11")
	s3.HandleAwsChunked(r)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "Hello World",
		"expected 'Hello World', got %q", string(data))
}

func TestAwsChunkedUnsigned(t *testing.T) {
	// unsigned variant: no ";chunk-signature=..." extension
	body := "3\r\n" +
		"foo" +
		"\r\n" +
		"3\r\n" +
		"bar" +
		"\r\n" +
		"0\r\n" +
		"\r\n"

	r := newChunkedRequest(body, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "6")
	tassert.Fatalf(t, s3.IsAwsChunked(r), "expected IsAwsChunked true")
	s3.HandleAwsChunked(r)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "foobar",
		"expected 'foobar', got %q", string(data))
}

func TestAwsChunkedEmptyPayload(t *testing.T) {
	body := "0;chunk-signature=sig\r\n" +
		"\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "0")
	s3.HandleAwsChunked(r)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(data) == 0, "expected empty payload, got %d bytes", len(data))
}

func TestAwsChunkedWithTrailers(t *testing.T) {
	body := "5;chunk-signature=sig1\r\n" +
		"hello" +
		"\r\n" +
		"0;chunk-signature=sig2\r\n" +
		"x-amz-checksum-crc32:abcd\r\n" +
		"x-amz-trailer-signature:efgh\r\n" +
		"\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", "5")
	s3.HandleAwsChunked(r)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "hello",
		"expected 'hello', got %q", string(data))
}

func TestAwsChunkedNoOp(t *testing.T) {
	r := &http.Request{
		Header: make(http.Header),
		Body:   io.NopCloser(strings.NewReader("plain data")),
	}
	r.Header.Set(cos.S3HdrContentSHA256, cos.S3UnsignedPayload)
	r.ContentLength = 10

	tassert.Fatalf(t, !s3.IsAwsChunked(r), "expected IsAwsChunked false for UNSIGNED-PAYLOAD")
	tassert.Fatalf(t, r.ContentLength == 10, "ContentLength should not change")

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, string(data) == "plain data", "body should be unmodified")
}

func TestAwsChunkedResetsContentSHA256(t *testing.T) {
	body := "0;chunk-signature=sig\r\n\r\n"
	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "0")
	s3.HandleAwsChunked(r)

	sha := r.Header.Get(cos.S3HdrContentSHA256)
	tassert.Fatalf(t, sha == cos.S3UnsignedPayload,
		"expected header reset to %q, got %q", cos.S3UnsignedPayload, sha)
}

// TestAwsChunkedHeaderAcrossRefill places the chunk header so it straddles the
// 4 KiB buffer boundary, forcing nextChunk to refill mid-header-parse.
func TestAwsChunkedHeaderAcrossRefill(t *testing.T) {
	const bufSize = s3.AwsChunkBufSize // 4096

	// First chunk: payload sized so that the second chunk's header starts
	// near the end of the first buffer fill and its '\n' lands after refill.
	//
	// Layout in the stream:
	//   [header1]  "a;chunk-signature=sig1\r\n"  (24 bytes)
	//   [data1]    10 bytes of 'A'
	//   [sep1]     "\r\n"                         (2 bytes)
	//   [padding]  fill up to bufSize-3 so the second header starts 3 bytes
	//              before the buffer boundary — "a;" fits, but the rest needs refill.
	//
	// We achieve this by making data1 large enough:
	//   24 + len(data1) + 2 = bufSize - 3
	//   len(data1) = bufSize - 29
	payloadLen := bufSize - 29
	payload := strings.Repeat("A", payloadLen)

	header2 := "5;chunk-signature=sig2\r\n"
	data2 := "Hello"
	terminator := "0;chunk-signature=sig3\r\n\r\n"

	body := fmt.Sprintf("%x;chunk-signature=sig1\r\n", payloadLen) +
		payload + "\r\n" +
		header2 + data2 + "\r\n" +
		terminator

	totalDecoded := strconv.Itoa(payloadLen + 5)
	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", totalDecoded)
	s3.HandleAwsChunked(r)

	data, err := io.ReadAll(r.Body)
	tassert.CheckFatal(t, err)

	expected := payload + "Hello"
	tassert.Fatalf(t, string(data) == expected,
		"payload mismatch: got %d bytes, want %d", len(data), len(expected))
}

// TestAwsChunkedTooManyHexDigits verifies that a malicious chunk size with more
// than 16 hex digits is rejected.
func TestAwsChunkedTooManyHexDigits(t *testing.T) {
	// 17 hex digits followed by a valid header terminator
	body := "00000000000000001;chunk-signature=sig\r\nX\r\n0;chunk-signature=sig\r\n\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "1")
	s3.HandleAwsChunked(r)

	_, err := io.ReadAll(r.Body)
	tassert.Fatalf(t, err != nil, "expected error for oversized hex chunk size")
	tassert.Fatalf(t, strings.Contains(err.Error(), "exceeds"),
		"expected 'exceeds' in error, got: %v", err)
}

// TestAwsChunkedTooManyHexDigitsAllZeros is a variant where all 17 digits are
// zeros — still must be rejected even though the numeric value is 0.
func TestAwsChunkedTooManyHexDigitsAllZeros(t *testing.T) {
	body := "00000000000000000;chunk-signature=sig\r\n\r\n"

	r := newChunkedRequest(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "0")
	s3.HandleAwsChunked(r)

	_, err := io.ReadAll(r.Body)
	tassert.Fatalf(t, err != nil, "expected error for oversized hex chunk size (all zeros)")
	tassert.Fatalf(t, strings.Contains(err.Error(), "exceeds"),
		"expected 'exceeds' in error, got: %v", err)
}

func newChunkedRequest(body, contentSHA256, decodedLen string) *http.Request {
	r := &http.Request{
		Header: make(http.Header),
		Body:   io.NopCloser(strings.NewReader(body)),
	}
	r.Header.Set(cos.S3HdrContentSHA256, contentSHA256)
	r.Header.Set(cos.S3HdrDecodedContentLength, decodedLen)
	r.Header.Set(cos.HdrContentLength, "999")
	r.ContentLength = 999
	return r
}
