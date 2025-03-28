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
	ups uploads
	mu  sync.RWMutex
)

// Start miltipart upload
func InitUpload(id, bckName, objName string, metadata map[string]string) {
	mu.Lock()
	if ups == nil {
		ups = make(uploads, 8)
	}
	ups[id] = &mpt{
		bckName:  bckName,
		objName:  objName,
		parts:    make([]*MptPart, 0, iniCapParts),
		ctime:    time.Now(),
		metadata: metadata,
	}
	mu.Unlock()
}

// Add part to an active upload.
// Some clients may omit size and md5. Only partNum is must-have.
// md5 and fqn is filled by a target after successful saving the data to a workfile.
func AddPart(id string, npart *MptPart) (err error) {
	mu.Lock()
	mpt, ok := ups[id]
	if !ok {
		err = fmt.Errorf("upload %q not found (%s, %d)", id, npart.FQN, npart.Num)
	} else {
		mpt.parts = append(mpt.parts, npart)
	}
	mu.Unlock()
	return
}

// TODO: compare non-zero sizes (note: s3cmd sends 0) and part.ETag as well, if specified
func CheckParts(id string, parts []types.CompletedPart) ([]*MptPart, error) {
	mu.RLock()
	defer mu.RUnlock()
	mpt, ok := ups[id]
	if !ok {
		return nil, fmt.Errorf("upload %q not found", id)
	}
	// first, check that all parts are present
	var prev = int32(-1)
	for _, part := range parts {
		curr := *part.PartNumber
		debug.Assert(curr > prev) // must ascend
		if mpt.getPart(curr) == nil {
			return nil, fmt.Errorf("upload %q: part %d not found", id, curr)
		}
		prev = curr
	}
	// copy (to work on it with no locks)
	nparts := make([]*MptPart, 0, len(parts))
	for _, part := range parts {
		nparts = append(nparts, mpt.getPart(*part.PartNumber))
	}
	return nparts, nil
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
func ObjSize(id string) (size int64, err error) {
	mu.RLock()
	mpt, ok := ups[id]
	if !ok {
		err = fmt.Errorf("upload %q not found", id)
	} else {
		for _, part := range mpt.parts {
			size += part.Size
		}
	}
	mu.RUnlock()
	return
}

func GetUploadMetadata(id string) (metadata map[string]string) {
	mu.RLock()
	defer mu.RUnlock()
	mpt, ok := ups[id]
	if !ok {
		return nil
	}
	return mpt.metadata
}

// remove all temp files and delete from the map
// if completed (i.e., not aborted): store xattr
func CleanupUpload(id, fqn string, aborted bool) (exists bool) {
	mu.Lock()
	mpt, ok := ups[id]
	if !ok {
		mu.Unlock()
		nlog.Warningf("fqn %s, id %s", fqn, id)
		return false
	}
	delete(ups, id)
	mu.Unlock()

	if !aborted {
		if err := storeMptXattr(fqn, mpt); err != nil {
			nlog.Warningln("failed to xattr [", fqn, id, err, "]")
		}
	}
	for _, part := range mpt.parts {
		if err := cos.RemoveFile(part.FQN); err != nil {
			nlog.Errorln("failed to remove part [", fqn, id, err, "]")
		}
	}
	return true
}

func ListUploads(bckName, idMarker string, maxUploads int) (result *ListMptUploadsResult) {
	mu.RLock()
	results := make([]UploadInfoResult, 0, len(ups))
	for id, mpt := range ups {
		results = append(results, UploadInfoResult{Key: mpt.objName, UploadID: id, Initiated: mpt.ctime})
	}
	mu.RUnlock()

	sort.Slice(results, func(i int, j int) bool {
		return results[i].Initiated.Before(results[j].Initiated)
	})

	var from int
	if idMarker != "" {
		// truncate
		for i, res := range results {
			if res.UploadID == idMarker {
				from = i + 1
				break
			}
			copy(results, results[from:])
			results = results[:len(results)-from]
		}
	}
	if maxUploads > 0 && len(results) > maxUploads {
		results = results[:maxUploads]
	}
	result = &ListMptUploadsResult{Bucket: bckName, Uploads: results, IsTruncated: from > 0}
	return
}

func ListParts(id string, lom *core.LOM) (parts []types.CompletedPart, ecode int, err error) {
	mu.RLock()
	mpt, ok := ups[id]
	if !ok {
		ecode = http.StatusNotFound
		mpt, err = loadMptXattr(lom.FQN)
		if err != nil || mpt == nil {
			mu.RUnlock()
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
	mu.RUnlock()
	return parts, ecode, err
}
