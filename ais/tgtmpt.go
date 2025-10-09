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
	"sort"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
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
	partArgs struct {
		r        *http.Request
		lom      *core.LOM
		manifest *core.Ufest
		chunk    *core.Uchunk
		fh       io.WriteCloser
		uploadID string
		partNum  int
		isS3     bool
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
func validateChecksumEtag(lom *core.LOM, manifest *core.Ufest, parts apc.MptCompletedParts, isS3 bool) (string, error) {
	if err := manifest.Check(); err != nil {
		return "", err
	}
	if _, err := _checkParts(parts, manifest.Count()); err != nil {
		return "", err
	}

	var (
		cksumH    *cos.CksumHash
		bck       = lom.Bck()
		remote    = bck.IsRemoteS3() || bck.IsRemoteOCI()
		cksumType = lom.CksumType()
	)
	// always bucket policy (and see [convention] below)
	if cksumType != cos.ChecksumNone {
		cksumH = cos.NewCksumHash(cksumType)
		if err := manifest.ComputeWholeChecksum(cksumH); err != nil {
			return "", err
		}
		lom.SetCksum(&cksumH.Cksum)
	}

	if remote /*computed by remote*/ || !isS3 /*native caller*/ {
		return "" /*etag*/, nil
	}

	// NOTE [convention]
	// for chunks `isS3` overrides bucket-configured checksum always requiring MD5
	// (see putPart)

	debug.Assert(manifest.Count() == 0 || len(manifest.GetChunk(1, true).MD5) == cos.LenMD5Hash)

	return manifest.ETagS3()
}

// make sure that we have all parts in the right order
func _checkParts(parts apc.MptCompletedParts, count int) (int, error) {
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

//
// upload API verbs: (start | putPart | complete | abort)
//

func (ups *ups) start(r *http.Request, lom *core.LOM) (uploadID string, err error) {
	uploadID, metadata, err := ups._start(r, lom)
	if err != nil {
		return
	}
	return uploadID, ups.init(uploadID, lom, metadata)
}

func (ups *ups) _start(r *http.Request, lom *core.LOM) (uploadID string, metadata map[string]string, err error) {
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

func (ups *ups) putPart(args *partArgs) (etag string, ecode int, err error) {
	var (
		uploadID = args.uploadID
		lom      = args.lom
	)
	manifest, _ := ups.get(uploadID, lom)
	if manifest == nil {
		return "", http.StatusNotFound, cos.NewErrNotFound(lom, uploadID)
	}
	args.manifest = manifest

	// new chunk
	if args.chunk, err = manifest.NewChunk(args.partNum, lom); err != nil {
		return "", http.StatusInternalServerError, err
	}

	path := args.chunk.Path()
	if args.fh, err = lom.CreatePart(path); err != nil {
		return "", http.StatusInternalServerError, err
	}

	etag, ecode, err = ups._put(args)
	cos.Close(args.fh)

	if err != nil {
		if nerr := cos.RemoveFile(path); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, ups.t, err, "remove", path, nerr)
		}
		return "", ecode, err
	}
	return etag, ecode, err
}

func (ups *ups) _put(args *partArgs) (etag string, ecode int, err error) {
	var (
		lom          = args.lom
		r            = args.r
		partSHA      = r.Header.Get(cos.S3HdrContentSHA256)
		checkPartSHA = partSHA != "" && partSHA != cos.S3UnsignedPayload
		cksumSHA     *cos.CksumHash
		cksumH       = &cos.CksumHash{}
		remote       = lom.Bck().IsRemote()
		writers      = make([]io.Writer, 0, 3)
		startTime    = mono.NanoTime()
	)
	if checkPartSHA {
		cksumSHA = cos.NewCksumHash(cos.ChecksumSHA256)
		writers = append(writers, cksumSHA.H)
	}
	if !remote {
		if args.isS3 {
			// NOTE [convention] for chunks `isS3` overrides bucket-configured checksum always requiring MD5
			cksumH = cos.NewCksumHash(cos.ChecksumMD5)
			writers = append(writers, cksumH.H)
		} else if ty := lom.CksumType(); ty != cos.ChecksumNone {
			debug.Assert(!checkPartSHA)
			cksumH = cos.NewCksumHash(ty)
			writers = append(writers, cksumH.H)
		}
	}
	writers = append(writers, args.fh)

	if r.ContentLength <= 0 {
		return "", http.StatusBadRequest, fmt.Errorf("%s: put-part invalid size (%d)", lom.Cname(), r.ContentLength)
	}

	// write
	// for remote buckets, use SGL buffering when memory is available;
	// fall back to TeeReader to avoid high memory usage under pressure
	var (
		backend          core.Backend
		uploadID         = args.uploadID
		expectedSize     int64
		remotePutLatency int64
		t                = ups.t
		mw               = cos.IniWriterMulti(writers...)
	)
	switch {
	case !remote:
		buf, slab := t.gmm.AllocSize(r.ContentLength)
		expectedSize, err = io.CopyBuffer(mw, r.Body, buf)
		slab.Free(buf)
	case lom.Bck().IsRemoteAzure():
		// NOTE: Azure backend requires io.ReadSeekCloser for the `PutMptPart` method
		backend = t.Backend(lom.Bck())
		sgl := t.gmm.NewSGL(r.ContentLength)
		mw.Append(sgl)
		reader := memsys.NewReader(sgl)
		expectedSize, err = io.Copy(mw, r.Body)
		if err == nil {
			remoteStart := mono.NanoTime()
			etag, ecode, err = backend.PutMptPart(lom, reader, r, uploadID, expectedSize, int32(args.partNum))
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	case t.gmm.Pressure() < memsys.PressureHigh:
		// write 1) locally + sgl + checksums; 2) write sgl => backend
		backend = t.Backend(lom.Bck())
		sgl := t.gmm.NewSGL(r.ContentLength)
		mw.Append(sgl)
		expectedSize, err = io.Copy(mw, r.Body)
		if err == nil {
			remoteStart := mono.NanoTime()
			etag, ecode, err = backend.PutMptPart(lom, sgl, r, uploadID, expectedSize, int32(args.partNum))
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	default:
		// utilize TeeReader to simultaneously write => backend
		backend = t.Backend(lom.Bck())
		expectedSize = r.ContentLength
		tr := io.NopCloser(io.TeeReader(r.Body, mw))
		remoteStart := mono.NanoTime()
		etag, ecode, err = backend.PutMptPart(lom, tr, r, uploadID, expectedSize, int32(args.partNum))
		remotePutLatency = mono.SinceNano(remoteStart)
	}

	// validate, finalize
	var (
		size  = mw.Size()
		chunk = args.chunk
	)
	if err == nil && size != expectedSize {
		err = fmt.Errorf("%s: part %d size mismatch (%d vs %d)", lom.Cname(), args.partNum, size, expectedSize)
		ecode = http.StatusBadRequest
	}
	if err != nil {
		if ecode == 0 {
			ecode = http.StatusInternalServerError
		}
		return "", ecode, err
	}

	// finalize (note: expecting the part's remote etag to be md5 checksum)
	if checkPartSHA {
		cksumSHA.Finalize()
		recvSHA := cos.NewCksum(cos.ChecksumSHA256, partSHA)
		if !cksumSHA.Equal(recvSHA) {
			detail := fmt.Sprintf("part %d", args.partNum)
			err = cos.NewErrDataCksum(&cksumSHA.Cksum, recvSHA, detail)
			return "", http.StatusBadRequest, err
		}
	}

	chunk.SetETag(etag)
	if cksumH.H != nil {
		if args.isS3 {
			debug.Assert(cksumH.Ty() == cos.ChecksumMD5) // see [convention] above
			chunk.MD5 = cksumH.H.Sum(nil)
			debug.Assert(len(chunk.MD5) == cos.LenMD5Hash, len(chunk.MD5))

			// derive etag from chunk MD5 but only when backend didn’t return the former
			if etag == "" {
				etag = cmn.MD5hashToETag(chunk.MD5)
			}
		} else {
			// NOTE: not populating chunk.MD5 even if bucket-configured checksum is md5
			debug.Assert(!checkPartSHA)
			cksumH.Finalize()
			chunk.SetCksum(&cksumH.Cksum)
		}
	}
	if checkPartSHA {
		chunk.SetCksum(&cksumSHA.Cksum)
	}

	if err := args.manifest.Add(chunk, size, int64(args.partNum)); err != nil {
		return "", http.StatusInternalServerError, err
	}

	// stats
	delta := mono.SinceNano(startTime)
	vlabs := xvlabs(lom.Bck())
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.PutSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatency, Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	if remotePutLatency > 0 {
		t.statsT.AddWith(
			cos.NamedVal64{Name: backend.MetricName(stats.PutSize), Value: size, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.PutLatencyTotal), Value: remotePutLatency, VarLabs: vlabs},
			cos.NamedVal64{Name: backend.MetricName(stats.PutE2ELatencyTotal), Value: delta, VarLabs: vlabs},
		)
	}

	return etag, ecode, nil
}

func (ups *ups) complete(r *http.Request, lom *core.LOM, uploadID string, body []byte, parts apc.MptCompletedParts, isS3 bool) (string, int, error) {
	t := ups.t
	manifest, remoteMeta := ups.get(uploadID, lom)
	if manifest == nil {
		return "", http.StatusNotFound, cos.NewErrNotFound(lom, uploadID)
	}

	// validate/enforce parts, compute whole-object checksum and etag
	manifest.Lock()
	etag, err := validateChecksumEtag(lom, manifest, parts, isS3)
	manifest.Unlock()
	if err != nil {
		return "", http.StatusBadRequest, err
	}

	// call remote
	remote := lom.Bck().IsRemote()
	if remote {
		// NOTE: only OCI, AWS and GCP backends require ETag in the part list
		if lom.Bck().IsRemoteS3() || lom.Bck().IsRemoteOCI() || lom.Bck().IsRemoteGCP() {
			for i := range parts {
				if parts[i].ETag == "" {
					parts[i].ETag = manifest.GetChunk(parts[i].PartNumber, true).ETag
				}
			}
		}

		tag, ecode, err := ups._completeRemote(r, lom, uploadID, body, parts)
		if err != nil {
			return "", ecode, err
		}
		etag = tag
	}

	if remote && remoteMeta != nil {
		md := ups.encodeRemoteMetadata(lom, remoteMeta)
		for k, v := range md {
			lom.SetCustomKey(k, v)
		}
	}
	lom.SetCustomKey(cmn.ETag, etag)

	// atomically flip: persist manifest, mark chunked, persist main
	if err = lom.CompleteUfest(manifest); err != nil {
		nlog.Errorf("upload %q: failed to complete %s locally: %v", uploadID, lom.Cname(), err)
		return "", http.StatusInternalServerError, err
	}

	ups.del(uploadID)

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln(uploadID, "completed")
	}

	// stats (note that size is already counted via putPart)
	vlabs := xvlabs(lom.Bck())
	t.statsT.IncWith(stats.PutCount, vlabs)
	if remote {
		t.statsT.IncWith(t.Backend(lom.Bck()).MetricName(stats.PutCount), vlabs)
	}

	return etag, 0, nil
}

func (ups *ups) _completeRemote(r *http.Request, lom *core.LOM, uploadID string, body []byte, partList apc.MptCompletedParts) (etag string, ecode int, err error) {
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

func (ups *ups) abort(r *http.Request, lom *core.LOM, uploadID string) (int, error) {
	if err := cos.ValidateManifestID(uploadID); err != nil {
		return http.StatusBadRequest, err
	}
	if lom.Bck().IsRemote() {
		ecode, err := ups.t.Backend(lom.Bck()).AbortMpt(lom, r, uploadID)

		if err == nil || ecode == http.StatusNotFound {
			if e := ups._abort(uploadID, lom); e != nil && !cos.IsNotExist(e) {
				nlog.Warningln("failed to abort: [", uploadID, lom.Cname(), e, "]")
			}
		}
		return ecode, err
	}

	if err := ups._abort(uploadID, lom); err != nil {
		if cos.IsNotExist(err) {
			return http.StatusNotFound, err
		}
		return http.StatusInternalServerError, err
	}
	return 0, nil
}

func (ups *ups) _abort(id string, lom *core.LOM) error {
	var (
		manifest *core.Ufest
	)
	ups.Lock()
	up, ok := ups.m[id]
	if !ok {
		ups.Unlock()
		m, err := ups.loadPartial(id, lom, false /*add*/)
		if err != nil {
			return err // may be a wrapped NotFound
		}
		manifest = m
	} else {
		manifest = up.u
		delete(ups.m, id)
		ups.Unlock()
	}

	manifest.Abort(lom)
	return nil
}
