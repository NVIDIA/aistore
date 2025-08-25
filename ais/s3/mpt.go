// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
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
				UploadID:  manifest.ID,
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
	parts = make([]types.CompletedPart, 0, len(manifest.Chunks))
	for i := range manifest.Chunks {
		c := &manifest.Chunks[i]
		etag := c.ETag
		if etag == "" {
			etag = cmn.MD5ToETag(c.MD5)
		}
		parts = append(parts, types.CompletedPart{
			ETag:       apc.Ptr(etag),
			PartNumber: apc.Ptr(int32(c.Num())),
		})
	}
	manifest.Unlock()
	return parts, 0, nil
}
