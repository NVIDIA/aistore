// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	manifest := core.NewUfest(id, lom, false /*must-exist*/)
	ups.Lock()
	if ups.m == nil {
		ups.m = make(map[string]up, iniCapUploads)
	}
	ups._add(id, manifest, rmd)
	ups.Unlock()
	return nil
}

func (ups *ups) _add(id string, manifest *core.Ufest, rmd map[string]string) {
	debug.Assert(manifest.Lom() != nil)
	_, ok := ups.m[id]
	debug.Assert(!ok, "duplicated upload ID: ", id, " ", manifest.Lom().Cname())
	ups.m[id] = up{manifest, rmd}
}

func (ups *ups) get(id string) (manifest *core.Ufest) {
	ups.RLock()
	manifest = ups.m[id].u
	ups.RUnlock()
	return
}

func (ups *ups) getWithMeta(id string) (manifest *core.Ufest, rmd map[string]string) {
	ups.RLock()
	manifest = ups.m[id].u
	rmd = ups.m[id].rmd
	ups.RUnlock()
	return
}

// NOTE: must be called with ups and lom both unlocked
func (ups *ups) loadPartial(id string, lom *core.LOM, add bool) (manifest *core.Ufest, err error) {
	debug.Assert(lom.IsLocked() == apc.LockNone, "expecting not locked: ", lom.Cname())

	lom.Lock(false)
	defer lom.Unlock(false)
	manifest = core.NewUfest(id, lom, true /*must-exist*/)
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

func (ups *ups) completeRemote(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values, uploadID string, body []byte, partList *s3.CompleteMptUpload) (etag string, err error) {
	var (
		ecode    int
		provider string
		version  string
		bck      = lom.Bck()
	)
	if bck.IsRemoteS3() {
		provider = apc.AWS
		version, etag, ecode, err = backend.CompleteMptAWS(lom, r, q, uploadID, body, partList)
	} else {
		debug.Assert(bck.IsRemoteOCI())
		provider = apc.OCI
		version, etag, ecode, err = backend.CompleteMptOCI(ups.t.Backend(lom.Bck()), lom, r, q, uploadID, body, partList)
	}
	if err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
	} else {
		lom.SetCustomKey(cmn.SourceObjMD, provider)
		if version != "" {
			lom.SetCustomKey(cmn.VersionObjMD, version)
		}
	}
	return
}

func (ups *ups) abortRemote(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values, uploadID string) (err error) {
	var (
		ecode int
		bck   = lom.Bck()
	)
	if bck.IsRemoteS3() {
		ecode, err = backend.AbortMptAWS(lom, r, q, uploadID)
	} else if bck.IsRemoteOCI() {
		ecode, err = backend.AbortMptOCI(ups.t.Backend(lom.Bck()), lom, r, q, uploadID)
	}
	if err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
	}
	return
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
