// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Root           = "s3"
	ContentType    = "application/xml"
	getContentType = "binary/octet-stream"
	acceptRanges   = "bytes"

	// versioning
	URLParamVersioning  = "versioning" // URL parameter
	URLParamMultiDelete = "delete"
	versioningEnabled   = "Enabled"
	versioningDisabled  = "Suspended"

	s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01"
	// TODO: can it be omitted? // storageClass = "STANDARD"

	// Headers
	headerSize         = "Content-Length"
	headerContentType  = "Content-Type"
	headerAcceptRanges = "Content-Range"
	headerContentRange = "Accept-Ranges"
	headerETag         = "ETag"
	headerVersion      = "x-amz-version-id"
	HeaderObjSrc       = "x-amz-copy-source"
	HeaderRange        = "Range"
	headerAtime        = "Last-Modified"
)

// Parses range in RFC2616 format(bytes=N-N, bytes=-N, bytes=N-), and returns
// the offset and length of an objects slice.
// If the end of the range exceeds object size, but start does not, the
// length of slice is corrected to make valid range.
func ParseS3Range(r string, objSize int64) (int64, int64, error) {
	parts := strings.Split(r, "=")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("%s - invalid range format", r)
	}
	// TODO: can parts[0] can be anything besides 'bytes'?
	// All examples uses only 'bytes'
	if parts[0] != "bytes" {
		return 0, 0, fmt.Errorf("%s - invalid bytes unit value", r)
	}
	if strings.HasPrefix(parts[1], "-") {
		// Case 1(last N bytes): bytes=-400
		// Parse from the second character to get positive value
		i, err := strconv.ParseInt(parts[1][1:], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		if i > objSize {
			return 0, 0, fmt.Errorf("range %s exceeds object size %d", parts[1], objSize)
		}
		return objSize - i, i, nil
	}
	sizes := strings.Split(parts[1], "-")
	if len(sizes) == 1 || sizes[1] == "" {
		// Case 2(from an offset to the end of object): bytes=300-
		i, err := strconv.ParseInt(sizes[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		if i >= objSize {
			return 0, 0, fmt.Errorf("range %d exceeds object size %d", i, objSize)
		}
		return i, objSize - i, nil
	}

	// Case 3(a slice in the middle of the object): bytes=100-400
	start, err := strconv.ParseInt(sizes[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start >= objSize {
		return 0, 0, fmt.Errorf("range start %d exceeds object size %d", start, objSize)
	}
	end, err := strconv.ParseInt(sizes[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if end >= objSize {
		end = objSize - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("range start %d is greater than its end %d", start, end)
	}
	return start, end - start + 1, nil
}
