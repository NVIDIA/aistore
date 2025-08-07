// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	iniCapUploads = 8
)

type uploads map[string]*core.Ufest // by upload ID

var (
	ups   uploads
	upsMu sync.RWMutex
	once  sync.Once
)

// - see tgts3mpt NOTE in re "active uploads in memory"
// - TODO: this is the place to call Ufest.Store
func InitUpload(id string, lom *core.LOM, metadata map[string]string) {
	// just once
	once.Do(func() {
		ups = make(uploads, iniCapUploads)
	})

	// Create chunk manifest
	manifest := core.NewUfest(id, lom)
	if metadata != nil {
		manifest.Metadata = metadata
	}

	upsMu.Lock()
	ups[id] = manifest
	upsMu.Unlock()
}

func ParsePartNum(s string) (int32, error) {
	partNum, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		err = fmt.Errorf("invalid part number %q (must be in 1-%d range): %v", s, MaxPartsPerUpload, err)
	}
	return int32(partNum), err
}

func GetUpload(id string) (manifest *core.Ufest) {
	upsMu.RLock()
	manifest = ups[id]
	upsMu.RUnlock()

	return
}

// - see tgts3mpt NOTE in re "active uploads in memory"
// - notwithstanding, keeping this from-disk loading code for future
func AbortUpload(id string, lom *core.LOM) (ecode int, err error) {
	debug.Assert(lom != nil)

	upsMu.Lock()
	manifest, ok := ups[id]
	if !ok {
		upsMu.Unlock()
		// Try to load from xattr
		manifest = core.NewUfest(id, lom)
		err = manifest.Load(lom)
		if err != nil {
			return http.StatusNotFound, NewErrNoSuchUpload(id)
		}
		upsMu.Lock()
	}
	delete(ups, id)
	upsMu.Unlock()

	if err := manifest.Abort(lom); err != nil {
		nlog.Warningln("failed to cleanup chunks [", id, lom.Cname(), err, "]")
		return http.StatusInternalServerError, err
	}

	return 0, nil
}

func DelCompletedUpload(id string) {
	upsMu.Lock()
	delete(ups, id)
	upsMu.Unlock()
}

// - see tgts3mpt NOTE in re "active uploads in memory"
// - notwithstanding, keeping this from-disk loading code for future
func ListUploads(bckName, idMarker string, maxUploads int) *ListMptUploadsResult {
	results := make([]UploadInfoResult, 0, len(ups))

	// lock all (notice performance trade-off)
	upsMu.RLock()
	// filter by bucket
	for id, manifest := range ups {
		if bckName == "" || manifest.Lom.Bck().Name == bckName {
			results = append(results, UploadInfoResult{
				Key:       manifest.Lom.ObjName,
				UploadID:  id,
				Initiated: manifest.StartTime,
			})
		}
	}
	upsMu.RUnlock()

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

// - see tgts3mpt NOTE in re "active uploads in memory"
// - notwithstanding, keeping this from-disk loading code for future
func ListParts(id string, lom *core.LOM) (parts []types.CompletedPart, ecode int, err error) {
	upsMu.RLock()
	manifest, ok := ups[id]
	upsMu.RUnlock()

	if !ok {
		manifest = core.NewUfest(id, lom)
		if err = manifest.Load(lom); err != nil {
			return nil, http.StatusNotFound, err
		}
		upsMu.Lock()
		ups[id] = manifest
		upsMu.Unlock()
	}

	manifest.Lock()
	parts = make([]types.CompletedPart, 0, len(manifest.Chunks))
	for _, c := range manifest.Chunks {
		parts = append(parts, types.CompletedPart{
			ETag:       apc.Ptr(c.MD5),
			PartNumber: apc.Ptr(int32(c.Num)),
		})
	}
	manifest.Unlock()
	return parts, 0, nil
}
