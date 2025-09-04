// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
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

// NOTE:
// - if not in memory may try to load from persistence given feat.ResumeInterruptedMPU
// - and, if successful, will return remoteMeta = nil
// TODO:
// - consider deriving remoteMeta from lom.GetCustomMD() vs the risk of getting out of sync with remote
// - consider adding stats counter mpu.resume_partial_count
func (ups *ups) get(id string, lom *core.LOM) (manifest *core.Ufest, remoteMeta map[string]string) {
	ups.RLock()
	up, ok := ups.m[id]
	ups.RUnlock()
	if ok {
		manifest = up.u
		remoteMeta = up.rmd
		debug.Assert(id == manifest.ID())
		return
	}
	if !lom.IsFeatureSet(feat.ResumeInterruptedMPU) {
		return
	}
	manifest, _ = ups.loadPartial(id, lom, true)
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
	if err = manifest.LoadPartial(lom); err != nil {
		return nil, err
	}

	if add {
		ups.Lock()
		if _, ok := ups.m[id]; !ok {
			ups._add(id, manifest, nil /*remote metadata*/)
		}
		ups.Unlock()
	}
	return manifest, nil
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

	manifest.Abort(lom)
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

func (ups *ups) start(r *http.Request, lom *core.LOM) (uploadID string, metadata map[string]string, err error) {
	bck := lom.Bck()
	if bck.IsRemote() {
		switch {
		case bck.IsRemoteS3():
			metadata = cmn.BackendHelpers.Amazon.DecodeMetadata(r.Header)
		case bck.IsRemoteOCI():
			metadata = cmn.BackendHelpers.OCI.DecodeMetadata(r.Header)
		}
		uploadID, _, err = ups.t.Backend(bck).StartMpt(lom, r)
	} else {
		uploadID = cos.GenUUID()
	}

	return uploadID, metadata, err
}

func (ups *ups) completeRemote(r *http.Request, lom *core.LOM, uploadID string, body []byte, partList apc.MptCompletedParts) (etag string, ecode int, err error) {
	var (
		version  string
		bck      = lom.Bck()
		provider = bck.Provider
	)

	version, etag, ecode, err = ups.t.Backend(bck).CompleteMpt(lom, r, uploadID, body, partList)
	if err != nil {
		return "", ecode, err
	}

	lom.SetCustomKey(cmn.SourceObjMD, provider)
	if version != "" {
		lom.SetCustomKey(cmn.VersionObjMD, version)
	}
	return etag, ecode, nil
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
