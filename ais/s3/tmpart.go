// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"os"
	"sync"
)

type (
	UploadPart struct {
		MD5  string // MD5 of the part
		FQN  string // FQN of the corresponding workfile
		Size int64  // part size in bytes
	}
	uploadInfo struct {
		objName string
		parts   map[int64]*UploadPart // part number <-> part info
	}
	uploads map[string]*uploadInfo // upload ID <-> upload info
)

var (
	up uploads
	mu sync.RWMutex
)

func Init() { up = make(uploads) }

// Start miltipart upload
func InitUpload(id, objName string) {
	mu.Lock()
	// TODO: as we generate ID internally, it is very unlikely that an upload with
	// the same ID exists. But we can check it anyway.
	up[id] = &uploadInfo{objName: objName, parts: make(map[int64]*UploadPart, 8)}
	mu.Unlock()
}

// Add part to an active upload.
// Some clients may omit size and md5. Only partNum is must-have.
// md5 and fqn is filled by a target after successful saving the data to a workfile.
func AddPart(id string, partNum, size int64, fqn, md5 string) (err error) {
	mu.Lock()
	upload, ok := up[id]
	if !ok {
		err = fmt.Errorf("upload %q not found", id)
	} else {
		upload.parts[partNum] = &UploadPart{Size: size, MD5: md5, FQN: fqn}
	}
	mu.Unlock()
	return
}

// Complete an upload and send the list of its parts.
// Some clients(e.g, s3cmd) omits some fields(e.g, size and md5).
// PartNumber is always filled.
// The function checks whether a target has all parts.
func CheckParts(id string, parts []*PartInfo) error {
	mu.RLock()
	defer mu.RUnlock()
	upload, ok := up[id]
	if !ok {
		return fmt.Errorf("upload %q not found", id)
	}
	if len(upload.parts) != len(parts) {
		return fmt.Errorf("upload %q expects %d parts, found %d parts", id, len(parts), len(upload.parts))
	}
	for _, part := range parts {
		_, ok := upload.parts[part.PartNumber]
		if !ok {
			return fmt.Errorf("upload %q: part %d not found", id, part.PartNumber)
		}
		// TODO: compare sizes? but is it filled by a client always? s3cmd seems sending 0 size
		// TODO: compare part.ETag with md5Part to be sure that the part is correct? but is it filled by a client always?
	}
	return nil
}

// Returns upload part that is already saved as a workfile.
func GetPart(id string, partNum int64) (*UploadPart, error) {
	mu.RLock()
	defer mu.RUnlock()
	upload, ok := up[id]
	if !ok {
		return nil, fmt.Errorf("upload %q not found", id)
	}
	part, ok := upload.parts[partNum]
	if !ok {
		return nil, fmt.Errorf("upload %q does not have part %d", id, partNum)
	}
	return part, nil
}

// Return a sum of upload part sizes.
// Used on upload completion to calculate the final size of the object.
func ObjSize(id string) (size int64, err error) {
	mu.RLock()
	upload, ok := up[id]
	if !ok {
		err = fmt.Errorf("upload %q not found", id)
	} else {
		for _, v := range upload.parts {
			size += v.Size
		}
	}
	mu.RUnlock()
	return
}

// Cleanup upload: remove all temp files and delete from the map
// (called in both cases: aborting and completing upload).
func RemoveUpload(id string) {
	mu.Lock()
	upload, ok := up[id]
	if !ok {
		// nothing to cleanup (TODO: target restart)
		mu.Unlock()
		return
	}
	for _, part := range upload.parts {
		_ = os.RemoveAll(part.FQN)
	}
	delete(up, id)
	mu.Unlock()
}

// Returns the info about active upload with ID
func GetUpload(id string) (upload *uploadInfo, err error) {
	mu.RLock()
	upload, err = _getup(id)
	mu.RUnlock()
	return
}

func _getup(id string) (*uploadInfo, error) {
	upload, ok := up[id]
	if !ok {
		return nil, fmt.Errorf("upload %q not found", id)
	}
	return upload, nil
}

// Returns true if there is active upload with ID
func UploadExists(id string) bool {
	mu.RLock()
	_, ok := up[id]
	mu.RUnlock()
	return ok
}

func ListUploads(bckName string) (result *ListMultipartUploadsResult) {
	mu.RLock()
	uploads := make([]*UploadInfo, 0, len(up))
	for id, info := range up {
		uploads = append(uploads, &UploadInfo{Key: info.objName, UploadID: id})
	}
	mu.RUnlock()
	result = &ListMultipartUploadsResult{Bucket: bckName, Uploads: uploads}
	return
}

func ListParts(id string) ([]*PartInfo, error) {
	mu.RLock()
	upload, err := _getup(id)
	if err != nil {
		mu.RUnlock()
		return nil, err
	}
	parts := make([]*PartInfo, 0, len(upload.parts))
	for num, part := range upload.parts {
		parts = append(parts, &PartInfo{ETag: part.MD5, PartNumber: num, Size: part.Size})
	}
	mu.RUnlock()
	return parts, nil
}
