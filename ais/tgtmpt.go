// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	iniCapUploads = 8
)

type (
	up struct {
		u   *core.Ufest
		rmd map[string]string
	}
	ups struct {
		t *target
		m map[string]up
		sync.RWMutex
	}
)

func (ups *ups) init(id string, lom *core.LOM, rmd map[string]string) error {
	manifest, err := core.NewUfest(id, lom, false /*must-exist*/)
	if err != nil {
		return err
	}
	ups.Lock()
	if ups.m == nil {
		ups.m = make(map[string]up, iniCapUploads)
	}
	err = ups._add(id, manifest, rmd)
	ups.Unlock()
	return err
}

func (ups *ups) _add(id string, manifest *core.Ufest, rmd map[string]string) (err error) {
	debug.Assert(manifest.Lom() != nil)
	if _, ok := ups.m[id]; ok {
		err = fmt.Errorf("duplicated upload ID: %q", id)
		debug.AssertNoErr(err)
		return
	}
	ups.m[id] = up{manifest, rmd}
	return
}

func (ups *ups) get(id string) (manifest *core.Ufest) {
	ups.RLock()
	up, ok := ups.m[id]
	if ok {
		manifest = up.u
		debug.Assert(id == manifest.ID())
	}
	ups.RUnlock()
	return
}

func (ups *ups) getWithMeta(id string) (manifest *core.Ufest, rmd map[string]string) {
	ups.RLock()
	up, ok := ups.m[id]
	if ok {
		manifest = up.u
		rmd = up.rmd
		debug.Assert(id == manifest.ID())
	}
	ups.RUnlock()
	return
}

// NOTE: must be called with ups and lom both unlocked
func (ups *ups) loadPartial(id string, lom *core.LOM, add bool) (manifest *core.Ufest, err error) {
	debug.Assert(lom.IsLocked() == apc.LockNone, "expecting not locked: ", lom.Cname())

	manifest, err = core.NewUfest(id, lom, true /*must-exist*/)
	if err != nil {
		return nil, err
	}

	lom.Lock(false)
	defer lom.Unlock(false)
	if err = manifest.LoadPartial(lom); err == nil && add {
		ups.Lock()
		ups._add(id, manifest, nil /*remote metadata <= LOM custom*/)
		ups.Unlock()
	}
	return
}

func (ups *ups) abort(id string, lom *core.LOM) (ecode int, err error) {
	var manifest *core.Ufest
	ups.Lock()
	up, ok := ups.m[id]
	if !ok {
		ups.Unlock()
		manifest, err = ups.loadPartial(id, lom, false /*add*/)
		if err != nil {
			return 0, err
		}
	} else {
		manifest = up.u
		delete(ups.m, id)
		ups.Unlock()
	}

	if err := manifest.Abort(lom); err != nil {
		nlog.Warningln("failed to cleanup chunks [", id, lom.Cname(), err, "]")
		return http.StatusInternalServerError, err
	}

	return 0, nil
}

func (ups *ups) toSlice() (all []*core.Ufest) {
	ups.RLock()
	all = make([]*core.Ufest, 0, len(ups.m))
	for _, up := range ups.m {
		all = append(all, up.u)
	}
	ups.RUnlock()
	return
}

func (ups *ups) del(id string) {
	ups.Lock()
	delete(ups.m, id)
	ups.Unlock()
}

//
// backend operations - encapsulate IsRemoteS3/IsRemoteOCI pattern
//

func (ups *ups) start(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values) (uploadID string, metadata map[string]string, err error) {
	var (
		ecode int
		bck   = lom.Bck()
	)
	switch {
	case bck.IsRemoteS3():
		metadata = cmn.BackendHelpers.Amazon.DecodeMetadata(r.Header)
		uploadID, ecode, err = backend.StartMptAWS(lom, r, q)
	case bck.IsRemoteOCI():
		metadata = cmn.BackendHelpers.OCI.DecodeMetadata(r.Header)
		uploadID, ecode, err = backend.StartMptOCI(ups.t.Backend(lom.Bck()), lom, r, q)
	default:
		uploadID = cos.GenUUID()
	}
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
	}
	return
}

func (ups *ups) putPartRemote(lom *core.LOM, reader io.ReadCloser, r *http.Request, q url.Values, uploadID string, expectedSize int64, partNum int32) (etag string, ecode int, err error) {
	bck := lom.Bck()
	if bck.IsRemoteS3() {
		etag, ecode, err = backend.PutMptPartAWS(lom, reader, r, q, uploadID, expectedSize, partNum)
	} else {
		debug.Assert(bck.IsRemoteOCI())
		etag, ecode, err = backend.PutMptPartOCI(ups.t.Backend(lom.Bck()), lom, reader, r, q, uploadID, expectedSize, partNum)
	}
	return
}

func (ups *ups) completeRemote(r *http.Request, lom *core.LOM, q url.Values, uploadID string, body []byte, partList apc.MptCompletedParts) (etag string, ecode int, err error) {
	var (
		provider string
		version  string
		bck      = lom.Bck()
		// TODO: do the `apc.MptCompletedParts` -> `types.CompletedPart` translation inside the backend provider's methods. remove `types` dependency in this file.
		s3Parts = &s3.CompleteMptUpload{
			Parts: make([]types.CompletedPart, partList.Len()),
		}
	)
	for i, part := range partList {
		pn := int32(part.PartNumber)
		s3Parts.Parts[i] = types.CompletedPart{
			PartNumber: &pn,
			ETag:       &part.ETag,
		}
	}
	if bck.IsRemoteS3() {
		provider = apc.AWS
		version, etag, ecode, err = backend.CompleteMptAWS(lom, r, q, uploadID, body, s3Parts)
	} else {
		debug.Assert(bck.IsRemoteOCI())
		provider = apc.OCI
		version, etag, ecode, err = backend.CompleteMptOCI(ups.t.Backend(lom.Bck()), lom, r, q, uploadID, body, s3Parts)
	}
	if err != nil {
		return "", ecode, err
	}
	lom.SetCustomKey(cmn.SourceObjMD, provider)
	if version != "" {
		lom.SetCustomKey(cmn.VersionObjMD, version)
	}
	return etag, ecode, nil
}

func (ups *ups) abortRemote(r *http.Request, lom *core.LOM, q url.Values, uploadID string) (ecode int, err error) {
	bck := lom.Bck()
	switch {
	case bck.IsRemoteS3():
		return backend.AbortMptAWS(lom, r, q, uploadID)
	case bck.IsRemoteOCI():
		return backend.AbortMptOCI(ups.t.Backend(lom.Bck()), lom, r, q, uploadID)
	default:
		return http.StatusNotImplemented, cmn.NewErrUnsupp("abort", bck.Provider+" multipart upload")
	}
}

func (*ups) encodeRemoteMetadata(lom *core.LOM, metadata map[string]string) (md map[string]string) {
	bck := lom.Bck()
	if bck.IsRemoteS3() {
		md = cmn.BackendHelpers.Amazon.EncodeMetadata(metadata)
	} else {
		debug.Assert(bck.IsRemoteOCI())
		md = cmn.BackendHelpers.OCI.EncodeMetadata(metadata)
	}
	return
}

//
// misc
//

func (*ups) parsePartNum(s string) (int32, error) {
	partNum, err := strconv.ParseInt(s, 10, 32)
	switch {
	case err != nil:
		return 0, fmt.Errorf("invalid part number %q: %v", s, err)
	case partNum < 1:
		return 0, fmt.Errorf("part number must be a positive integer, got: %d", partNum)
	default:
		return int32(partNum), nil
	}
}

// (under manifest lock)
// TODO: only for checking ETag and checksum for S3 backend, not needed for the native MPT APIs
func validateChecksumEtag(lom *core.LOM, manifest *core.Ufest, parts apc.MptCompletedParts) (string, error) {
	if err := manifest.Check(); err != nil {
		return "", err
	}
	if _, err := enforceCompleteAllParts(parts, manifest.Count()); err != nil {
		return "", err
	}

	// compute "whole" checksum (TODO: sha256 may take precedence when implied by `partSHA`)
	var (
		wholeCksum *cos.CksumHash
		bck        = lom.Bck()
		remote     = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if remote && lom.CksumConf().Type != cos.ChecksumNone {
		wholeCksum = cos.NewCksumHash(lom.CksumConf().Type)
	} else {
		wholeCksum = cos.NewCksumHash(cos.ChecksumMD5)
	}
	if wholeCksum != nil {
		if err := manifest.ComputeWholeChecksum(wholeCksum); err != nil {
			return "", err
		}
		lom.SetCksum(&wholeCksum.Cksum)
	}

	// compute multipart-compliant ETag if need be
	var etag string
	if !remote {
		var err error
		if etag, err = manifest.ETagS3(); err != nil {
			return "", err
		}
	}

	return etag, nil
}

func enforceCompleteAllParts(parts apc.MptCompletedParts, count int) (int, error) {
	if parts == nil {
		return http.StatusBadRequest, errors.New("nil parts list")
	}
	if len(parts) != count {
		return http.StatusNotImplemented,
			fmt.Errorf("partial completion is not allowed: requested %d parts, have %d",
				len(parts), count)
	}
	// fast path
	for i := range count {
		p := parts[i]
		if p.PartNumber != i+1 {
			goto slow
		}
	}
	return 0, nil

slow:
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	for i := range count {
		p := parts[i]
		got := p.PartNumber
		if got != i+1 {
			return http.StatusBadRequest, fmt.Errorf("parts must be exactly 1..%d: got %d at position %d",
				count, got, i)
		}
	}
	return 0, nil
}
