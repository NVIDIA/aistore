// Package apc: API control messages and constants
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const _bldl = "blob-downloader"

type BlobMsg struct {
	ChunkSize  int64 `json:"chunk-size"`
	FullSize   int64 `json:"full-size"`
	NumWorkers int   `json:"num-workers"`
	LatestVer  bool  `json:"latest-ver"`
}

func (msg *BlobMsg) FromHeader(hdr http.Header) error {
	snw, schunk := hdr.Get(HdrBlobWorkers), hdr.Get(HdrBlobChunk)
	if snw != "" {
		nw, err := strconv.ParseInt(snw, 10, 16)
		if err != nil {
			return fmt.Errorf("%s: failed to parse %s=%s: %v", _bldl, HdrBlobWorkers, snw, err)
		}
		if nw < 0 || nw > 128 {
			return fmt.Errorf("%s: invalid %s=%s: expecting (0..128) range", _bldl, HdrBlobWorkers, snw)
		}
		msg.NumWorkers = int(nw)
	}
	if schunk == "" {
		return nil
	}
	chunk, err := cos.ParseSize(schunk, "")
	if err != nil {
		return fmt.Errorf("%s: failed to parse %s=%s: %v", _bldl, HdrBlobChunk, schunk, err)
	}
	msg.ChunkSize = chunk
	return nil
}
