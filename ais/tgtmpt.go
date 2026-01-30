// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

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
		req         *http.Request
		reader      io.ReadCloser
		lom         *core.LOM
		manifest    *core.Ufest
		chunk       *core.Uchunk
		fh          io.WriteCloser
		uploadID    string
		partNum     int
		size        int64 // take precedence over req.ContentLength
		isS3        bool
		skipBackend bool
	}
	completeArgs struct {
		r           *http.Request
		lom         *core.LOM
		uploadID    string
		body        []byte
		parts       apc.MptCompletedParts
		isS3        bool
		skipBackend bool
		locked      bool // true if the LOM is already locked by the caller
	}
	// partCksums holds checksum state for a single part upload
	partCksums struct {
		crc32c  *cos.CksumHash // always computed for local/remote-AIS (for combination)
		md5     *cos.CksumHash // S3 ETag compatibility
		sha256  *cos.CksumHash // S3 content-SHA256 header validation
		partSHA string         // expected SHA256 from request header
		inOrder bool           // true if this part uses streaming checksum
	}
)

func (ups *ups) init(id string, lom *core.LOM, rmd map[string]string) error {
	manifest, err := core.NewUfest(id, lom, false /*must-exist*/)
	if err != nil {
		return err
	}
	// Initialize streaming checksum for bucket-configured type
	manifest.InitStreamingChecksum(lom.CksumType())
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
	switch {
	case bck.IsRemoteS3():
		md = cmn.BackendHelpers.Amazon.EncodeMetadata(metadata)
	case bck.IsRemoteOCI():
		md = cmn.BackendHelpers.OCI.EncodeMetadata(metadata)
	default:
		// For other remotes (e.g., remais), return metadata as-is
		md = metadata
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

// validate manifest + parts; optionally compute S3 MPU ETag
// must be called under manifest lock; does NOT touch LOM or reread data
func validatePartsEtag(lom *core.LOM, manifest *core.Ufest, parts apc.MptCompletedParts, isS3 bool) (etag string, err error) {
	if err := manifest.Check(true /*completed*/); err != nil {
		return "", err
	}
	if _, err := _checkParts(parts, manifest.Count()); err != nil {
		return "", err
	}

	// NOTE:
	// ETag is only needed for S3-compat MPU on local buckets.
	// For cloud buckets, backend is authoritative (and returns its own ETag).

	if !isS3 || lom.Bck().IsCloud() {
		return "", nil
	}
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

slow: // slow path (same +sort)
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

func (ups *ups) start(r *http.Request, lom *core.LOM, skipBackend bool) (uploadID string, err error) {
	uploadID, metadata, err := ups._start(r, lom, skipBackend)
	if err != nil {
		return
	}
	return uploadID, ups.init(uploadID, lom, metadata)
}

func (ups *ups) _start(r *http.Request, lom *core.LOM, skipBackend bool) (uploadID string, metadata map[string]string, err error) {
	bck := lom.Bck()
	if bck.IsRemote() && !skipBackend {
		// Extract metadata:
		// - from HTTP request headers if available (normal upload path)
		// - from LOM's existing custom metadata if no request (e.g., rechunk SyncRemote)
		switch {
		case r != nil && bck.IsRemoteS3():
			metadata = cmn.BackendHelpers.Amazon.DecodeMetadata(r.Header)
		case r != nil && bck.IsRemoteOCI():
			metadata = cmn.BackendHelpers.OCI.DecodeMetadata(r.Header)
		default:
			metadata = lom.GetCustomMD()
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
		lom       = args.lom
		reader    = args.reader
		rsize     = args.size
		startTime = mono.NanoTime()
	)

	// Initialize checksums and get writers
	pc, writers := initPartChecksums(args)

	if rsize <= 0 {
		return "", http.StatusBadRequest, fmt.Errorf("%s: put-part invalid size (%d)", lom.Cname(), rsize)
	}

	// write
	// for remote buckets, use SGL buffering when memory is available
	var (
		backend          core.Backend
		remote           = lom.Bck().IsRemote()
		uploadID         = args.uploadID
		expectedSize     int64
		remotePutLatency int64
		t                = ups.t
		mw               = cos.IniWriterMulti(writers...)
	)
	switch {
	case !remote || args.skipBackend:
		// no need to write to backend
		buf, slab := t.gmm.AllocSize(rsize)
		expectedSize, err = io.CopyBuffer(mw, reader, buf)
		slab.Free(buf)
	case t.gmm.Pressure() < memsys.PressureHigh:
		// write 1) locally + sgl + checksums; 2) write sgl => backend
		backend = t.Backend(lom.Bck())
		sgl := t.gmm.NewSGL(rsize)
		mw.Append(sgl)
		expectedSize, err = io.Copy(mw, reader)
		if err == nil {
			// NOTE: memsys.Reader wrapper required:
			// - Azure: seekable (io.ReadSeekCloser) for SDK's seek-based retry
			// - Remote AIS: retriable (cos.ReadOpenCloser) for DoWithRetry (reader.Open() on retry)
			rdr := memsys.NewReader(sgl)
			remoteStart := mono.NanoTime()
			etag, ecode, err = backend.PutMptPart(lom, rdr, args.req, uploadID, expectedSize, int32(args.partNum))
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	default:
		// high memory pressure
		time.Sleep(cos.PollSleepLong) // wait for memory pressure to decrease
		if t.gmm.Pressure() >= memsys.PressureHigh {
			e := fmt.Errorf("memory pressure is too high: %d, put part rejected", t.gmm.Pressure())
			nlog.Errorln(e)
			err = cmn.NewErrTooManyRequests(e, http.StatusTooManyRequests)
			ecode = http.StatusTooManyRequests
			break
		}

		backend = t.Backend(lom.Bck())
		sgl := t.gmm.NewSGL(rsize)
		mw.Append(sgl)
		expectedSize, err = io.Copy(mw, reader)
		if err == nil {
			rdr := memsys.NewReader(sgl)
			remoteStart := mono.NanoTime()
			etag, ecode, err = backend.PutMptPart(lom, rdr, args.req, uploadID, expectedSize, int32(args.partNum))
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	}

	// Release streaming checksum lock
	if pc.inOrder {
		args.manifest.StreamWriteDone()
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

	// Finalize checksums (validate SHA256, store CRC32C and MD5)
	etag, err = pc.finalize(chunk, args.partNum, etag)
	if err != nil {
		return "", http.StatusBadRequest, err
	}
	chunk.SetETag(etag)

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

func (ups *ups) complete(args *completeArgs) (string, int, error) {
	var (
		lom      = args.lom
		uploadID = args.uploadID
		t        = ups.t
	)
	manifest, remoteMeta := ups.get(uploadID, lom)
	if manifest == nil {
		return "", http.StatusNotFound, cos.NewErrNotFound(lom, uploadID)
	}

	// validate/enforce parts, compute etag
	manifest.Lock()
	etag, err := validatePartsEtag(lom, manifest, args.parts, args.isS3)
	manifest.Unlock()
	if err != nil {
		return "", http.StatusBadRequest, err
	}

	// call remote
	remote := lom.Bck().IsRemote()
	if remote && !args.skipBackend { // skipBackend implies no need to write to backend
		// NOTE: only OCI, AWS and GCP backends require ETag in the part list
		if lom.Bck().IsRemoteS3() || lom.Bck().IsRemoteOCI() || lom.Bck().IsRemoteGCP() {
			for i := range args.parts {
				if args.parts[i].ETag == "" {
					c, err := manifest.GetChunk(args.parts[i].PartNumber)
					debug.AssertNoErr(err)
					args.parts[i].ETag = c.ETag
				}
			}
		}

		tag, ecode, err := ups._completeRemote(args.r, lom, uploadID, args.body, args.parts)
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
	if etag != "" {
		lom.SetCustomKey(cmn.ETag, cmn.UnquoteCEV(etag))
	}

	// Whole-object checksum: use streaming checksum if valid, otherwise CRC32C combination
	var locked bool
	if !args.locked {
		lom.Lock(true)
		locked = true
	}

	cksum, err := manifest.WholeChecksum()
	if err != nil {
		if locked {
			lom.Unlock(true)
		}
		return "", 0, err
	}
	lom.SetCksum(cksum)

	// atomically flip: persist manifest, mark chunked, persist main
	// NOTE: coldGET implies the LOM's lock has been promoted to wlock
	err = lom.CompleteUfest(manifest, args.locked || locked)
	if locked {
		lom.Unlock(true)
	}
	if err != nil {
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

	return cmn.QuoteETag(etag), 0, nil
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

//
// part checksums
//

// initPartChecksums determines which checksums to compute and returns writers for them.
// Checksum strategy:
// - Always compute CRC32C for chunk combination (cheap, can be combined without re-reading)
// - If parts arrive in-order, also compute streaming bucket-configured checksum
func initPartChecksums(args *partArgs) (pc partCksums, w []io.Writer) {
	w = make([]io.Writer, 0, 4)
	w = append(w, args.fh)

	// S3 content-SHA256 header validation
	if args.req != nil {
		pc.partSHA = args.req.Header.Get(cos.S3HdrContentSHA256)
		if pc.partSHA != "" && pc.partSHA != cos.S3UnsignedPayload {
			pc.sha256 = cos.NewCksumHash(cos.ChecksumSHA256)
			w = append(w, pc.sha256.H)
		}
	}

	// Always compute CRC32C for chunk combination
	pc.crc32c = cos.NewCksumHash(cos.ChecksumCRC32C)
	w = append(w, pc.crc32c.H)

	// Check if part arrives in-order for streaming bucket-configured checksum
	var streamWriter hash.Hash
	pc.inOrder, streamWriter = args.manifest.CheckInOrder(args.partNum)
	if streamWriter != nil {
		w = append(w, streamWriter)
	}

	// S3 also needs MD5 for ETag compatibility
	if args.isS3 {
		pc.md5 = cos.NewCksumHash(cos.ChecksumMD5)
		w = append(w, pc.md5.H)
	}

	return pc, w
}

// finalize validates and stores checksums in the chunk
func (pc *partCksums) finalize(chunk *core.Uchunk, partNum int, etag string) (string, error) {
	// Validate SHA256 if provided in request header
	if pc.sha256 != nil {
		pc.sha256.Finalize()
		expected := cos.NewCksum(cos.ChecksumSHA256, pc.partSHA)
		if !pc.sha256.Equal(expected) {
			return "", cos.NewErrDataCksum(&pc.sha256.Cksum, expected, fmt.Sprintf("part %d", partNum))
		}
	}

	// Store CRC32C in chunk for combination
	if pc.crc32c != nil {
		pc.crc32c.Finalize()
		chunk.SetCksum(&pc.crc32c.Cksum)
	}

	// S3 compatibility API over ais:// buckets: compute part ETag as MD5 of the part (S3 convention).
	if pc.md5 != nil {
		chunk.MD5 = pc.md5.H.Sum(nil)
		debug.Assert(len(chunk.MD5) == cos.LenMD5Hash, len(chunk.MD5))
		etag = cmn.MD5ToQuotedETag(chunk.MD5)
	}
	return etag, nil
}
