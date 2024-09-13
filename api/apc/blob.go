// Package apc: API control messages and constants
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
	"net/http"
	"net/textproto"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const _bldl = "blob-downloader"

type BlobMsg struct {
	ChunkSize  int64 `json:"chunk-size"`  // as in: chunk size
	FullSize   int64 `json:"full-size"`   // user-specified (full) size of the object to download
	NumWorkers int   `json:"num-workers"` // number of concurrent downloading workers (readers); `dfltNumWorkers` when zero
	LatestVer  bool  `json:"latest-ver"`  // when true and in-cluster: check with remote whether (deleted | version-changed)
}

// in re LatestVer, see also: `QparamLatestVer`, 'versioning.validate_warm_get'

// using textproto.CanonicalMIMEHeaderKey() to check presence -
// if a given key is present and is an empty string, it's an error
func (msg *BlobMsg) FromHeader(hdr http.Header) error {
	canWorkers := textproto.CanonicalMIMEHeaderKey(HdrBlobWorkers)
	valWorkers, okw := hdr[canWorkers]
	if okw {
		// single value
		nw, err := strconv.ParseInt(valWorkers[0], 10, 16)
		if err != nil {
			return fmt.Errorf("%s: failed to parse %s=%s: %v", _bldl, HdrBlobWorkers, valWorkers[0], err)
		}
		if nw < 0 || nw > 128 {
			return fmt.Errorf("%s: invalid %s=%s: expecting (0..128) range", _bldl, HdrBlobWorkers, valWorkers[0])
		}
		msg.NumWorkers = int(nw)
	}

	canChunkSz := textproto.CanonicalMIMEHeaderKey(HdrBlobChunk)
	valChunkSz, okz := hdr[canChunkSz]
	if !okz {
		return nil
	}
	// single value
	chunk, err := cos.ParseSize(valChunkSz[0], "")
	if err != nil {
		return fmt.Errorf("%s: failed to parse %s=%s: %v", _bldl, HdrBlobChunk, valChunkSz[0], err)
	}
	msg.ChunkSize = chunk
	return nil
}
