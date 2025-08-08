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

// see also: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	maxPartsPerUpload = 10000
)

type (
	uploads struct {
		t *target
		m map[string]*core.Ufest // by upload ID
		sync.RWMutex
	}
)

func (ups *uploads) init(id string, lom *core.LOM, metadata map[string]string) {
	manifest := core.NewUfest(id, lom)
	manifest.Metadata = metadata

	ups.Lock()
	if ups.m == nil {
		ups.m = make(map[string]*core.Ufest, iniCapUploads)
	}
	ups._add(id, manifest)
	ups.Unlock()
}

func (ups *uploads) _add(id string, manifest *core.Ufest) {
	debug.Assert(manifest.Lom != nil)
	_, ok := ups.m[id]
	debug.Assert(!ok, "duplicated upload ID: ", id, " ", manifest.Lom.Cname())
	ups.m[id] = manifest
}

func (ups *uploads) get(id string) (manifest *core.Ufest) {
	ups.RLock()
	manifest = ups.m[id]
	ups.RUnlock()
	return
}

// NOTE: must be called with ups unlocked
func (ups *uploads) fromFS(id string, lom *core.LOM, add bool) (manifest *core.Ufest, err error) {
	manifest = &core.Ufest{ID: id}
	if err = manifest.Load(lom); err == nil && add {
		ups.Lock()
		ups._add(id, manifest)
		ups.Unlock()
	}
	return
}

func (ups *uploads) abort(id string, lom *core.LOM) (ecode int, err error) {
	ups.Lock()
	manifest, ok := ups.m[id]
	if !ok {
		ups.Unlock()
		manifest, err = ups.fromFS(id, lom, false /*add*/)
		if err != nil {
			return 0, err
		}
	} else {
		delete(ups.m, id)
		ups.Unlock()
	}

	if err := manifest.Abort(lom); err != nil {
		nlog.Warningln("failed to cleanup chunks [", id, lom.Cname(), err, "]")
		return http.StatusInternalServerError, err
	}

	return 0, nil
}

func (ups *uploads) toSlice() (all []*core.Ufest) {
	ups.RLock()
	all = make([]*core.Ufest, 0, len(ups.m))
	for _, manifest := range ups.m {
		all = append(all, manifest)
	}
	ups.RUnlock()
	return
}

func (ups *uploads) del(id string) {
	ups.Lock()
	delete(ups.m, id)
	ups.Unlock()
}

//
// backend operations - encapsulate IsRemoteS3/IsRemoteOCI pattern
//

func (ups *uploads) start(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values) (uploadID string, metadata map[string]string, err error) {
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

func (ups *uploads) putPartRemote(lom *core.LOM, reader io.ReadCloser, r *http.Request, q url.Values, uploadID string, expectedSize int64, partNum int32) (etag string, ecode int, err error) {
	bck := lom.Bck()
	if bck.IsRemoteS3() {
		etag, ecode, err = backend.PutMptPartAWS(lom, reader, r, q, uploadID, expectedSize, partNum)
	} else {
		debug.Assert(bck.IsRemoteOCI())
		etag, ecode, err = backend.PutMptPartOCI(ups.t.Backend(lom.Bck()), lom, reader, r, q, uploadID, expectedSize, partNum)
	}
	return
}

func (ups *uploads) completeRemote(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values, uploadID string, body []byte, partList *s3.CompleteMptUpload) (etag string, err error) {
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

func (ups *uploads) abortRemote(w http.ResponseWriter, r *http.Request, lom *core.LOM, q url.Values, uploadID string) (err error) {
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

func (*uploads) encodeRemoteMetadata(lom *core.LOM, metadata map[string]string) (md map[string]string) {
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

func (*uploads) parsePartNum(s string) (int32, error) {
	partNum, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		err = fmt.Errorf("invalid part number %q (must be in 1-%d range): %v", s, maxPartsPerUpload, err)
	}
	return int32(partNum), err
}
