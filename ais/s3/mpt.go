// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type ListMptUploadsParams struct {
	BckName        string
	Prefix         string
	KeyMarker      string
	UploadIDMarker string
	MaxUploads     int
}

// ParseMptMaxUploads validates the S3 max-uploads request parameter.
func ParseMptMaxUploads(value string) (int, error) {
	if value == "" {
		return apc.MaxPageSizeAWS, nil
	}
	maxUploads, err := strconv.Atoi(value)
	if err != nil || maxUploads < 1 || maxUploads > apc.MaxPageSizeAWS {
		return 0, fmt.Errorf("invalid %q=%q: expecting an integer between 1 and %d",
			QparamMptMaxUploads, value, apc.MaxPageSizeAWS)
	}
	return maxUploads, nil
}

func ListUploads(all []*core.Ufest, params ListMptUploadsParams) *ListMptUploadsResult {
	results := make([]UploadInfoResult, 0, len(all))

	// filter by bucket
	for _, manifest := range all {
		lom := manifest.Lom()
		if params.BckName == "" || lom.Bck().Name == params.BckName {
			results = append(results, UploadInfoResult{
				Key:       lom.ObjName,
				UploadID:  manifest.ID(),
				Initiated: manifest.Created(),
			})
		}
	}
	return PageUploads(results, params)
}

// PageUploads merges target-local multipart upload listings and applies S3
// filtering and pagination to the cluster-wide result.
func PageUploads(results []UploadInfoResult, params ListMptUploadsParams) *ListMptUploadsResult {
	results = slices.Clone(results)
	if params.Prefix != "" {
		filtered := results[:0]
		for _, result := range results {
			if strings.HasPrefix(result.Key, params.Prefix) {
				filtered = append(filtered, result)
			}
		}
		results = filtered
	}

	// sort by (object name, initiation time)
	sort.Slice(results, func(i, j int) bool {
		if results[i].Key != results[j].Key {
			return results[i].Key < results[j].Key
		}
		if !results[i].Initiated.Equal(results[j].Initiated) {
			return results[i].Initiated.Before(results[j].Initiated)
		}
		return results[i].UploadID < results[j].UploadID
	})

	// Start after the key/upload pair returned by the previous page. When only
	// key-marker is present, skip all uploads for that key as required by S3.
	if params.KeyMarker != "" {
		from := sort.Search(len(results), func(i int) bool { return results[i].Key >= params.KeyMarker })
		if params.UploadIDMarker == "" {
			for from < len(results) && results[from].Key == params.KeyMarker {
				from++
			}
		} else {
			found := false
			for i := from; i < len(results) && results[i].Key == params.KeyMarker; i++ {
				if results[i].UploadID == params.UploadIDMarker {
					from = i + 1
					found = true
					break
				}
			}
			// The marked upload may have completed or been aborted between pages.
			// Its initiation time is no longer available, so skip the rest of that
			// key to avoid repeating uploads already returned by an earlier page.
			if !found {
				for from < len(results) && results[from].Key == params.KeyMarker {
					from++
				}
			}
		}
		if from > 0 {
			results = results[from:]
		}
	}

	// apply maxUploads limit
	var truncated bool
	if len(results) > params.MaxUploads {
		results = results[:params.MaxUploads]
		truncated = true
	}

	result := &ListMptUploadsResult{
		Bucket:         params.BckName,
		KeyMarker:      params.KeyMarker,
		UploadIDMarker: params.UploadIDMarker,
		Prefix:         params.Prefix,
		Uploads:        results,
		MaxUploads:     params.MaxUploads,
		IsTruncated:    truncated,
	}
	if truncated {
		last := results[len(results)-1]
		result.NextKeyMarker = last.Key
		result.NextUploadIDMarker = last.UploadID
	}
	return result
}

func ListParts(manifest *core.Ufest) (parts []types.CompletedPart, ecode int, err error) {
	manifest.Lock()
	parts = make([]types.CompletedPart, 0, manifest.Count())
	for i := range manifest.Count() {
		c, err := manifest.GetChunk(i + 1)
		if err != nil {
			return nil, http.StatusNotFound, err
		}
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
