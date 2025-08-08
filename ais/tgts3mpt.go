// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

// NOTE -- TODO: S3 Multipart Upload Implementation
//
// 1. in-memory state
//    - active uploads kept purely in-memory (ups map)
//    - no support for target restart recovery during active uploads
//    - if target restarts during upload, client must restart the entire upload
//    - this simplifies design and avoids complex state recovery mechanisms
//    - rationale: most multipart uploads complete within hours; target restarts
//      are rare operational events. the complexity of persistent state recovery
//      outweighs the benefit for the typical use case.
//
// 2. cleanup
//    - no automatic cleanup of abandoned uploads
//    - uploads remain in memory until explicitly completed or aborted
//
// 3. parts ordering
//    - completeMpt enforces strictly ascending, contiguous parts (1..n)
//    - this is stricter than aws s3, which allows gaps and sparse part sets
//    - rationale: simplifies chunk manifest structure and append logic;
//      most s3 clients upload parts sequentially anyway
//
// 4. concurrency
//    - global rwmutex for upload cache operations
//    - individual manifest mutexes for chunk operations
//    - tradeoff: simple consistency vs potential lock contention at scale
//
// 5. error handling
//    - fail fast on inconsistencies (size mismatches, missing parts)
//    - atomic operations where possible (chunk replacement, manifest storage)
//    - orphaned chunks cleaned up only on explicit abort, not on errors
//
// 6. persistence
//    - chunk manifests persisted to xattr only after successful completion
//    - used later for individual chunk access (getMptPart)
//    - not used for active upload state recovery
//    - failed/aborted uploads clean up chunks but don't persist manifests

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

// TODO:
// - remove appendChunks; make Ufest a reader
// - test

func decodeXML[T any](body []byte) (result T, _ error) {
	if err := xml.Unmarshal(body, &result); err != nil {
		return result, err
	}
	return result, nil
}

// Initialize multipart upload.
// - Generate UUID for the upload
// - Return the UUID to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (t *target) startMpt(w http.ResponseWriter, r *http.Request, items []string, bck *meta.Bck, q url.Values) {
	var (
		objName  = s3.ObjName(items)
		lom      = &core.LOM{ObjName: objName}
		metadata map[string]string
		uploadID string
	)
	err := lom.InitBck(bck.Bucket())
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID, metadata, err = t.ups.start(w, r, lom, q)
	if err != nil {
		return
	}
	t.ups.init(uploadID, lom, metadata)
	result := &s3.InitiateMptUploadResult{Bucket: bck.Name, Key: objName, UploadID: uploadID}

	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// PUT a part of the multipart upload.
// Body is empty, everything in the query params and the header.
//
// "Content-MD5" in the part headers seems be to be deprecated:
// either not present (s3cmd) or cannot be trusted (aws s3api).
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
func (t *target) putMptPart(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *meta.Bck) {
	var (
		remotePutLatency int64
		startTime        = mono.NanoTime()
	)
	// 1. parse/validate
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		s3.WriteErr(w, r, errors.New("empty uploadId"), 0)
		return
	}
	part := q.Get(s3.QparamMptPartNo)
	if part == "" {
		s3.WriteErr(w, r, fmt.Errorf("upload %q: missing part number", uploadID), 0)
		return
	}
	partNum, err := t.ups.parsePartNum(part)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if partNum < 1 || partNum > maxPartsPerUpload {
		err := fmt.Errorf("upload %q: invalid part number %d, must be between 1 and %d",
			uploadID, partNum, maxPartsPerUpload)
		s3.WriteErr(w, r, err, 0)
		return
	}

	// 2. init lom, load/create chunk manifest
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// TODO: compare with listMptUploads (that does fromFS)
	manifest := t.ups.get(uploadID)
	if manifest == nil {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), http.StatusNotFound, lom, uploadID)
		return
	}

	// Generate chunk file path
	chunkPath, err := manifest.ChunkName(int(partNum))
	if err != nil {
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}

	partFh, errC := lom.CreatePart(chunkPath)
	if errC != nil {
		s3.WriteMptErr(w, r, errC, 0, lom, uploadID)
		return
	}

	var (
		etag         string
		expectedSize int64
		ecode        int
		partSHA      = r.Header.Get(cos.S3HdrContentSHA256)
		checkPartSHA = partSHA != "" && partSHA != cos.S3UnsignedPayload
		cksumSHA     = &cos.CksumHash{}
		cksumMD5     = &cos.CksumHash{}
		remote       = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if checkPartSHA {
		cksumSHA = cos.NewCksumHash(cos.ChecksumSHA256)
	}
	if !remote {
		cksumMD5 = cos.NewCksumHash(cos.ChecksumMD5)
	}

	if r.ContentLength <= 0 {
		err := fmt.Errorf("put-part invalid size (%d)", r.ContentLength)
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// 3. write
	// for remote buckets, use SGL buffering when memory is available;
	// fall back to TeeReader to avoid high memory usage under pressure
	mw := cos.IniWriterMulti(cksumMD5.H, cksumSHA.H, partFh)

	switch {
	case !remote:
		buf, slab := t.gmm.AllocSize(r.ContentLength)
		expectedSize, err = io.CopyBuffer(mw, r.Body, buf)
		slab.Free(buf)
	case t.gmm.Pressure() < memsys.PressureHigh:
		// write 1) locally + sgl + checksums; 2) write sgl => backend
		sgl := t.gmm.NewSGL(r.ContentLength)
		mw.Append(sgl)
		expectedSize, err = io.Copy(mw, r.Body)
		if err == nil {
			remoteStart := mono.NanoTime()
			etag, ecode, err = t.ups.putPartRemote(lom, sgl, r, q, uploadID, expectedSize, partNum)
			remotePutLatency = mono.SinceNano(remoteStart)
		}
		sgl.Free()
	default:
		// utilize TeeReader to simultaneously write => backend
		expectedSize = r.ContentLength
		tr := io.NopCloser(io.TeeReader(r.Body, mw))
		remoteStart := mono.NanoTime()
		etag, ecode, err = t.ups.putPartRemote(lom, tr, r, q, uploadID, expectedSize, partNum)
		remotePutLatency = mono.SinceNano(remoteStart)
	}

	cos.Close(partFh)
	if err != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, t, err, "remove", chunkPath, nerr)
		}
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	size := mw.Size()
	if size != expectedSize {
		err := fmt.Errorf("put-part size mismatch (%d vs %d)", size, expectedSize)
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// 4. finalize the part (expecting the part's remote etag to be md5 checksum)
	md5 := cmn.UnquoteCEV(etag)
	if cksumMD5.H != nil {
		debug.Assert(etag == "")
		cksumMD5.Finalize()
		md5 = cksumMD5.Value()
	}
	if checkPartSHA {
		cksumSHA.Finalize()
		recvSHA := cos.NewCksum(cos.ChecksumSHA256, partSHA)
		if !cksumSHA.Equal(recvSHA) {
			detail := fmt.Sprintf("upload %q, %s, part %d", uploadID, lom, partNum)
			err = cos.NewErrDataCksum(&cksumSHA.Cksum, recvSHA, detail)
			s3.WriteMptErr(w, r, err, http.StatusInternalServerError, lom, uploadID)
			return
		}
	}

	chunk := &core.Uchunk{
		MD5:      md5,
		Path:     chunkPath,
		Siz:      size,
		Num:      uint16(partNum),
		CksumVal: md5,
	}

	// - see NOTE above in re "active uploads in memory"
	// - TODO: this is the place to call Ufest.Store(partial manifest)
	if err := manifest.Add(chunk); err != nil {
		s3.WriteMptErr(w, r, err, 0, lom, uploadID)
		return
	}
	w.Header().Set(cos.S3CksumHeader, md5) // s3cmd checks this one

	delta := mono.SinceNano(startTime)
	vlabs := map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.PutSize, Value: size, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatency, Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.PutLatencyTotal, Value: delta, VarLabs: vlabs},
	)
	if remotePutLatency > 0 {
		backendBck := t.Backend(bck)
		t.statsT.AddWith(
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutSize), Value: size, VarLabs: vlabs},
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutLatencyTotal), Value: remotePutLatency, VarLabs: vlabs},
			cos.NamedVal64{Name: backendBck.MetricName(stats.PutE2ELatencyTotal), Value: delta, VarLabs: vlabs},
		)
	}
}

// Complete multipart upload.
// Body contains XML with the list of parts that must be on the storage already.
// 1. Check that all parts from request body present
// 2. Merge all parts into a single file and calculate its ETag
// 3. Return ETag to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
func (t *target) completeMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *meta.Bck) {
	// parse/validate
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		s3.WriteErr(w, r, errors.New("empty uploadId"), 0)
		return
	}

	body, err := cos.ReadAllN(r.Body, r.ContentLength)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	partList, err := decodeXML[*s3.CompleteMptUpload](body)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusBadRequest)
		return
	}
	if len(partList.Parts) == 0 {
		s3.WriteErr(w, r, fmt.Errorf("upload %q: empty list of upload parts", uploadID), http.StatusBadRequest)
		return
	}
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName} // TODO: use core.AllocLOM()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	// TODO: compare with listMptUploads (that does fromFS)
	manifest := t.ups.get(uploadID)
	if manifest == nil {
		s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, nil), http.StatusNotFound, lom, uploadID)
		return
	}

	// call remote
	var (
		etag    string
		started = time.Now()
		remote  = bck.IsRemoteS3() || bck.IsRemoteOCI()
	)
	if remote {
		var err error
		etag, err = t.ups.completeRemote(w, r, lom, q, uploadID, body, partList)
		if err != nil {
			return
		}
	}

	// append parts and finalize locally
	var (
		mw          io.Writer
		actualCksum *cos.CksumHash
		numParts    = len(partList.Parts)
	)

	if numParts == 0 {
		s3.WriteMptErr(w, r, errors.New("empty parts list"), 0, lom, uploadID)
		return
	}
	sort.Slice(partList.Parts, func(i, j int) bool {
		return *partList.Parts[i].PartNumber < *partList.Parts[j].PartNumber
	})

	for i, p := range partList.Parts {
		if p.PartNumber == nil {
			s3.WriteMptErr(w, r, fmt.Errorf("nil part number at index %d", i), http.StatusBadRequest, lom, uploadID)
			return
		}
		want := int32(i + 1)
		got := *p.PartNumber
		if got != want {
			s3.WriteMptErr(w, r,
				fmt.Errorf("parts must be strictly ascending and contiguous (1..N): got %d at position %d", got, i),
				http.StatusBadRequest, lom, uploadID)
			return
		}
	}
	manifest.Lock()
	if len(manifest.Chunks) < numParts {
		manifest.Unlock()
		s3.WriteMptErr(w, r,
			fmt.Errorf("have %d parts, requested %d", len(manifest.Chunks), numParts),
			http.StatusBadRequest, lom, uploadID)
		return
	}
	nparts := make([]*core.Uchunk, numParts)
	for i := range numParts {
		c := &manifest.Chunks[i]
		if c.Num != uint16(i+1) {
			manifest.Unlock()
			s3.WriteMptErr(w, r, fmt.Errorf("missing or out-of-order part %d (found %d)", i+1, c.Num), 0, lom, uploadID)
			return
		}
		nparts[i] = c
	}

	metadata := manifest.Metadata
	manifest.Unlock()

	// 2. Create final work file
	prefix := uploadID + ".complete"
	wfqn := fs.CSM.Gen(lom, fs.WorkfileType, prefix)
	wfh, errC := lom.CreateWork(wfqn)
	if errC != nil {
		s3.WriteMptErr(w, r, errC, 0, lom, uploadID)
		return
	}
	if remote && lom.CksumConf().Type != cos.ChecksumNone {
		actualCksum = cos.NewCksumHash(lom.CksumConf().Type)
	} else {
		actualCksum = cos.NewCksumHash(cos.ChecksumMD5)
	}
	mw = cos.IniWriterMulti(actualCksum.H, wfh)

	// .3 write - append chunks in order
	buf, slab := t.gmm.Alloc()
	multichunkMD5, written, errA := _finalizeChunks(nparts, buf, mw)
	slab.Free(buf)

	if lom.IsFeatureSet(feat.FsyncPUT) {
		errS := wfh.Sync()
		debug.AssertNoErr(errS)
	}
	cos.Close(wfh)

	if errA == nil && written <= 0 {
		errA = fmt.Errorf("upload %q %q: invalid full size %d", uploadID, lom.Cname(), written)
	}
	if errA != nil {
		if nerr := cos.RemoveFile(wfqn); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorf(fmtNested, t, errA, "remove", wfqn, nerr)
		}
		s3.WriteMptErr(w, r, errA, 0, lom, uploadID)
		return
	}

	// .4 (s3 client => ais://) compute resulting MD5 and, optionally, ETag
	if actualCksum.H != nil {
		actualCksum.Finalize()
		lom.SetCksum(actualCksum.Cksum.Clone())
	}
	if etag == "" {
		debug.Assert(!remote)
		debug.Assert(multichunkMD5 != nil)

		multichunkMD5.Finalize()
		etag = `"` + multichunkMD5.Val() + cmn.AwsMultipartDelim + strconv.Itoa(len(nparts)) + `"`
	}

	// .5 finalize
	lom.SetSize(written)
	if remote {
		md := t.ups.encodeRemoteMetadata(lom, metadata)
		for k, v := range md {
			lom.SetCustomKey(k, v)
		}
	}
	lom.SetCustomKey(cmn.ETag, etag)

	poi := allocPOI()
	{
		poi.t = t
		poi.atime = started.UnixNano()
		poi.lom = lom
		poi.workFQN = wfqn
		poi.owt = cmn.OwtNone
	}
	ecode, errF := poi.finalize()
	freePOI(poi)

	// .6 write manifest
	if err := manifest.Store(lom); err != nil {
		s3.WriteMptErr(w, r, errF, ecode, lom, uploadID)
		return
	}
	t.ups.del(uploadID)

	if errF != nil {
		// NOTE: not failing if remote op. succeeded
		if !remote {
			s3.WriteMptErr(w, r, errF, ecode, lom, uploadID)
			return
		}
		nlog.Errorf("upload %q: failed to complete %s locally: %v(%d)", uploadID, lom.Cname(), errF, ecode)
	}

	// .7 respond
	result := &s3.CompleteMptUploadResult{Bucket: bck.Name, Key: objName, ETag: etag}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	s3.SetS3Headers(w.Header(), lom)
	sgl.WriteTo2(w)
	sgl.Free()

	// stats
	vlabs := map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	t.statsT.IncWith(stats.PutCount, vlabs)
	if remote {
		t.statsT.IncWith(t.Backend(bck).MetricName(stats.PutCount), vlabs)
	}
}

func _finalizeChunks(nparts []*core.Uchunk, buf []byte, mw io.Writer) (cksum *cos.CksumHash, written int64, err error) {
	cksum = cos.NewCksumHash(cos.ChecksumMD5)
	for _, c := range nparts {
		fh, err := os.Open(c.Path)
		if err != nil {
			return nil, 0, err
		}
		n, err := io.CopyBuffer(mw, fh, buf)
		cos.Close(fh)
		if err != nil {
			return nil, 0, err
		}
		if n != c.Siz {
			return nil, 0, fmt.Errorf("invalid size for c %d: %d vs %d", c.Num, n, c.Siz)
		}
		written += n

		bin, derr := hex.DecodeString(c.MD5)
		if derr != nil || len(bin) != md5.Size {
			return nil, 0, fmt.Errorf("invalid MD5 for c %d: %q", c.Num, c.MD5)
		}
		if _, err := cksum.H.Write(bin); err != nil {
			return nil, 0, err
		}
	}
	return cksum, written, nil
}

// Abort an active multipart upload.
// Body is empty, only URL query contains uploadID
// 1. uploadID must exists
// 2. Remove all temporary files
// 3. Remove all info from in-memory structs
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
func (t *target) abortMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	bck, ecode, err := meta.InitByNameOnly(items[0], t.owner.bmd)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	objName := s3.ObjName(items)
	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID := q.Get(s3.QparamMptUploadID)
	if bck.IsRemote() {
		if err := t.ups.abortRemote(w, r, lom, q, uploadID); err != nil {
			return
		}
	}
	if ecode, err := t.ups.abort(uploadID, lom); err != nil {
		s3.WriteMptErr(w, r, err, ecode, lom, uploadID)
		return
	}

	// Respond with status 204(!see the docs) and empty body.
	w.WriteHeader(http.StatusNoContent)
}

// List already stored parts of the active multipart upload by bucket name and uploadID.
// (NOTE: `s3cmd` lists upload parts before checking if any parts can be skipped.)
// s3cmd is OK to receive an empty body in response with status=200. In this
// case s3cmd sends all parts.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (t *target) listMptParts(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string, q url.Values) {
	uploadID := q.Get(s3.QparamMptUploadID)

	lom := &core.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	manifest := t.ups.get(uploadID)
	if manifest == nil {
		var err error
		if manifest, err = t.ups.fromFS(uploadID, lom, true /*add*/); err != nil {
			s3.WriteMptErr(w, r, s3.NewErrNoSuchUpload(uploadID, err), http.StatusNotFound, lom, uploadID)
			return
		}
	}

	parts, ecode, err := s3.ListParts(manifest)
	if err != nil {
		s3.WriteErr(w, r, err, ecode)
		return
	}
	result := &s3.ListPartsResult{Bucket: bck.Name, Key: objName, UploadID: uploadID, Parts: parts}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// List all active multipart uploads for a bucket.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
// GET /?uploads&delimiter=Delimiter&encoding-type=EncodingType&key-marker=KeyMarker&
// max-uploads=MaxUploads&prefix=Prefix&upload-id-marker=UploadIdMarker
func (t *target) listMptUploads(w http.ResponseWriter, bck *meta.Bck, q url.Values) {
	var (
		maxUploads int
		idMarker   string
	)
	if s := q.Get(s3.QparamMptMaxUploads); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			maxUploads = v
		}
	}
	idMarker = q.Get(s3.QparamMptUploadIDMarker)
	all := t.ups.toSlice()
	result := s3.ListUploads(all, bck.Name, idMarker, maxUploads)
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo2(w)
	sgl.Free()
}

// Acts on an already multipart-uploaded object, returns `partNumber` (URL query)
// part of the object.
// The object must have been multipart-uploaded beforehand.
// See:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (t *target) getMptPart(w http.ResponseWriter, r *http.Request, bck *meta.Bck, lom *core.LOM, q url.Values) {
	startTime := mono.NanoTime()
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	partNum, err := t.ups.parsePartNum(q.Get(s3.QparamMptPartNo))
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	lom.Lock(false)
	defer lom.Unlock(false)

	// load chunk manifest and find out the part num's offset & size
	manifest := core.NewUfest("", lom) // ID will be loaded from xattr
	err = manifest.Load(lom)
	if err != nil {
		s3.WriteErr(w, r, err, http.StatusNotFound)
		return
	}

	// Find the specific chunk
	manifest.Lock()
	chunk := manifest.GetChunk(uint16(partNum), true)
	manifest.Unlock()
	if chunk == nil {
		err := fmt.Errorf("part %d not found", partNum)
		s3.WriteErr(w, r, err, http.StatusNotFound)
		return
	}

	// Read chunk file directly
	fh, err := os.Open(chunk.Path)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	defer cos.Close(fh)

	buf, slab := t.gmm.AllocSize(chunk.Siz)
	defer slab.Free(buf)

	if _, err := io.CopyBuffer(w, fh, buf); err != nil {
		s3.WriteErr(w, r, err, 0)
	}

	vlabs := map[string]string{stats.VlabBucket: bck.Cname("")}
	t.statsT.IncWith(stats.GetCount, vlabs)
	t.statsT.AddWith(
		cos.NamedVal64{Name: stats.GetSize, Value: chunk.Siz, VarLabs: vlabs},
		cos.NamedVal64{Name: stats.GetLatencyTotal, Value: mono.SinceNano(startTime), VarLabs: vlabs},
	)
}
