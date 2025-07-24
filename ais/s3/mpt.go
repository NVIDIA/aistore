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
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	iniCapUploads = 8
	iniCapParts   = 4
)

// NOTE: xattr stores only the (*) marked attributes
type (
	MptPart struct {
		MD5  string // MD5 of the part (*)
		FQN  string // FQN of the corresponding workfile
		Size int64  // part size in bytes (*)
		Num  int32  // part number (*)
	}
	mpt struct {
		ctime    time.Time // InitUpload time
		bckName  string
		objName  string
		parts    []*MptPart // by part number
		metadata map[string]string
	}
	uploads map[string]*mpt // by upload ID
)

var (
	ups   uploads
	upsMu sync.RWMutex
	once  sync.Once
)

// Start multipart upload
func InitUpload(id, bckName, objName string, metadata map[string]string) {
	// just once
	once.Do(func() {
		ups = make(uploads, iniCapUploads)
	})

	upsMu.Lock()
	ups[id] = &mpt{
		bckName:  bckName,
		objName:  objName,
		parts:    make([]*MptPart, 0, iniCapParts),
		ctime:    time.Now(),
		metadata: metadata,
	}
	upsMu.Unlock()
}

// Add part to an active upload.
// Some clients may omit size and md5. Only partNum is must-have.
// md5 and fqn is filled by a target after successful saving the data to a workfile.
func AddPart(id string, npart *MptPart) (ecode int, err error) {
	upsMu.Lock()
	defer upsMu.Unlock()
	mpt, ok := ups[id]
	if !ok {
		return http.StatusNotFound, NewErrNoSuchUpload(id)
	}
	mpt.parts = append(mpt.parts, npart)
	return 0, nil
}

// TODO: compare non-zero sizes (note: s3cmd sends 0) and part.ETag as well, if specified
func CheckParts(id string, parts []types.CompletedPart) (nparts []*MptPart, ecode int, err error) {
	upsMu.RLock()
	defer upsMu.RUnlock()
	mpt, ok := ups[id]
	if !ok {
		return nil, http.StatusNotFound, NewErrNoSuchUpload(id)
	}

	// first, check that all parts are present
	var prev = int32(-1)
	for _, part := range parts {
		curr := *part.PartNumber
		debug.Assert(curr > prev) // must ascend
		if mpt.getPart(curr) == nil {
			return nil, http.StatusBadRequest, fmt.Errorf("upload %q: part %d not found", id, curr)
		}
		prev = curr
	}
	// copy (to work on it with no locks)
	nparts = make([]*MptPart, 0, len(parts))
	for _, part := range parts {
		nparts = append(nparts, mpt.getPart(*part.PartNumber))
	}
	return nparts, 0, nil
}

func ParsePartNum(s string) (int32, error) {
	partNum, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		err = fmt.Errorf("invalid part number %q (must be in 1-%d range): %v", s, MaxPartsPerUpload, err)
	}
	return int32(partNum), err
}

// Return a sum of upload part sizes.
// Used on upload completion to calculate the final size of the object.
func ObjSize(id string) (size int64, ecode int, err error) {
	upsMu.RLock()
	defer upsMu.RUnlock()
	mpt, ok := ups[id]
	if !ok {
		return 0, http.StatusNotFound, NewErrNoSuchUpload(id)
	}
	for _, part := range mpt.parts {
		size += part.Size
	}
	return size, 0, nil
}

func UploadExists(id string) bool {
	upsMu.RLock()
	_, ok := ups[id]
	upsMu.RUnlock()
	return ok
}

func GetUploadMetadata(id string) (metadata map[string]string, ecode int, err error) {
	upsMu.RLock()
	defer upsMu.RUnlock()
	mpt, ok := ups[id]
	if !ok {
		return nil, http.StatusNotFound, NewErrNoSuchUpload(id)
	}
	return mpt.metadata, 0, nil
}

// remove all temp files and delete from the map
// if completed (i.e., not aborted): store xattr
func CleanupUpload(id string, lom *core.LOM, aborted bool) (ecode int, err error) {
	debug.Assert(lom != nil)
	upsMu.Lock()
	mpt, ok := ups[id]
	if !ok {
		upsMu.Unlock()
		return http.StatusNotFound, NewErrNoSuchUpload(id)
	}
	delete(ups, id)
	upsMu.Unlock()

	if !aborted {
		if err := storeMptXattr(lom, mpt); err != nil {
			nlog.Warningln("failed to xattr [", id, lom.Cname(), err, "]")
		}
	}
	for _, part := range mpt.parts {
		if err := cos.RemoveFile(part.FQN); err != nil {
			nlog.Errorln("failed to remove part [", id, part.FQN, lom.Cname(), err, "]")
		}
	}
	return
}

func ListUploads(bckName, idMarker string, maxUploads int) *ListMptUploadsResult {
	results := make([]UploadInfoResult, 0, len(ups))

	// lock all (notice performance trade-off)
	upsMu.RLock()
	// filter by bucket
	for id, mpt := range ups {
		if bckName == "" || mpt.bckName == bckName {
			results = append(results, UploadInfoResult{
				Key:       mpt.objName,
				UploadID:  id,
				Initiated: mpt.ctime,
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

func ListParts(id string, lom *core.LOM) (parts []types.CompletedPart, ecode int, err error) {
	upsMu.RLock()
	mpt, ok := ups[id]
	if !ok {
		ecode = http.StatusNotFound
		mpt, err = loadMptXattr(lom)
		if err != nil || mpt == nil {
			upsMu.RUnlock()
			return nil, ecode, err
		}
		mpt.bckName, mpt.objName = lom.Bck().Name, lom.ObjName
		mpt.ctime = lom.Atime()
	}
	parts = make([]types.CompletedPart, 0, len(mpt.parts))
	for _, part := range mpt.parts {
		parts = append(parts, types.CompletedPart{
			ETag:       apc.Ptr(part.MD5),
			PartNumber: apc.Ptr(part.Num),
		})
	}
	upsMu.RUnlock()
	return parts, ecode, err
}
