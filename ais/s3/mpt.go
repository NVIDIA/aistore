// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func ListUploads(all []*core.Ufest, bckName, idMarker string, maxUploads int) *ListMptUploadsResult {
	results := make([]UploadInfoResult, 0, len(all))

	// filter by bucket
	for _, manifest := range all {
		lom := manifest.Lom()
		if bckName == "" || lom.Bck().Name == bckName {
			results = append(results, UploadInfoResult{
				Key:       lom.ObjName,
				UploadID:  manifest.ID(),
				Initiated: manifest.Created(),
			})
		}
	}

	// sort by (object name, initiation time)
	sort.Slice(results, func(i, j int) bool {
		if results[i].Key != results[j].Key {
			return results[i].Key < results[j].Key
		}
		return results[i].Initiated.Before(results[j].Initiated)
	})

	// paginate with idMarker if specified
	var from int
	if idMarker != "" {
		for i, res := range results {
			if res.UploadID == idMarker {
				from = i + 1
				break
			}
		}
		if from > 0 {
			results = results[from:]
		}
	}

	// apply maxUploads limit
	var truncated bool
	if maxUploads > 0 && len(results) > maxUploads {
		results = results[:maxUploads]
		truncated = true
	}

	return &ListMptUploadsResult{
		Bucket:      bckName,
		Uploads:     results,
		IsTruncated: truncated,
	}
}

func ListParts(manifest *core.Ufest) (parts []types.CompletedPart, ecode int, err error) {
	manifest.Lock()
	parts = make([]types.CompletedPart, 0, manifest.Count())
	for i := range manifest.Count() {
		c := manifest.GetChunk(i+1, true)
		etag := c.ETag
		if etag == "" {
			if c.MD5 != nil {
				debug.Assert(len(c.MD5) == cos.LenMD5Hash)
				etag = cmn.MD5ToQuotedETag(c.MD5)
			}
		} else {
			etag = cmn.QuoteETag(etag)
		}
		parts = append(parts, types.CompletedPart{
			ETag:       apc.Ptr(etag),
			PartNumber: apc.Ptr(int32(c.Num())),
		})
	}
	manifest.Unlock()
	return parts, 0, nil
}

// validate that the caller requests completion of
// exactly all uploaded parts: the set {1..count} in ANY order
// on success, normalize req.Parts in-place
// on error return:
// - 501 when partial completion is requested (len != count)
// - 400 for malformed input (nil part number, out of range, duplicates)
func EnforceCompleteAllParts(req *CompleteMptUpload, count int) (int, error) {
	if req == nil {
		return http.StatusBadRequest, errors.New("nil parts list")
	}
	if len(req.Parts) != count {
		return http.StatusNotImplemented,
			fmt.Errorf("partial completion is not allowed: requested %d parts, have %d",
				len(req.Parts), count)
	}
	// fast path
	for i := range count {
		p := req.Parts[i]
		if p.PartNumber == nil {
			return http.StatusBadRequest, fmt.Errorf("nil part number at index %d", i)
		}
		if *p.PartNumber != int32(i+1) {
			goto slow
		}
	}
	return 0, nil

slow:
	sort.Slice(req.Parts, func(i, j int) bool {
		pi, pj := req.Parts[i], req.Parts[j]
		// nil can't occur here due to the fast-path scan, but be defensive
		if pi.PartNumber == nil {
			return false
		}
		if pj.PartNumber == nil {
			return true
		}
		return *pi.PartNumber < *pj.PartNumber
	})
	for i := range count {
		p := req.Parts[i]
		if p.PartNumber == nil {
			return http.StatusBadRequest, fmt.Errorf("nil part number after sort at index %d", i)
		}
		got := *p.PartNumber
		if got != int32(i+1) {
			return http.StatusBadRequest, fmt.Errorf("parts must be exactly 1..%d: got %d at position %d",
				count, got, i)
		}
	}
	return 0, nil
}
