// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// Copy another object or its range as a part of the multipart upload.
// Body is empty, everything in the query params and the header.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
func (t *target) putObjMptCopy(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		t.writeErrMsg(w, r, "bucket and object names are required to complete multipart upload",
			http.StatusBadRequest)
		return
	}
	t.writeErrMsg(w, r, "not implemented yet")
}

// PUT a part of the multipart upload.
// Body is empty, everything in the query params and the header.
// While API states about "Content-MD5" is in the request, it looks like
// s3cmd does not set it.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
func (t *target) putObjMptPart(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	if len(items) < 2 {
		t.writeErrMsg(w, r, "bucket and object names are required to complete multipart upload",
			http.StatusBadRequest)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		t.writeErrMsg(w, r, "empty uploadId")
		return
	}
	part := q.Get(s3.QparamMptPartNo)
	if part == "" {
		t.writeErrMsg(w, r, "empty part number")
		return
	}
	partNum, err := strconv.ParseInt(part, 10, 16)
	if err != nil {
		t.writeErrf(w, r, "%v (must be between 1 and %d)", err, s3.MaxPartsPerUpload)
		return
	}
	if partNum < 1 || partNum > s3.MaxPartsPerUpload {
		t.writeErrStatusf(w, r, http.StatusBadRequest,
			"invalid part number %d, must be between 1 and %d", partNum, s3.MaxPartsPerUpload)
		return
	}
	if r.Header.Get(s3.HdrObjSrc) != "" {
		t.writeErrMsg(w, r, "uploading a copy is not supported yet", http.StatusNotImplemented)
		return
	}
	// TODO: it is empty for s3cmd. It seems s3cmd does not send MD5.
	//       Check if s3cmd sets Header.ETag with MD5.
	// TODO: s3cmd sends this one for every part, can we use it?
	//       sha256 := r.Header.Get(s3.HdrContentSHA256)

	partMD5 := r.Header.Get(cmn.AzCksumHeader)
	recvMD5 := cos.NewCksum(cos.ChecksumMD5, partMD5)

	bckName := items[0]
	objName := strings.Join(items[1:], "/")
	bck, err := newBckFromQ(bckName, r.URL.Query(), nil)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := &cluster.LOM{ObjName: objName}
	err = lom.InitBck(bck.Bucket())
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	// Temporary file name format: <upload-id>.<part-number>.<obj-name>
	prefix := fmt.Sprintf("%s.%d", uploadID, partNum)
	workfileFQN := fs.CSM.Gen(lom, fs.WorkfileType, prefix)
	file, err := os.Create(workfileFQN)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	cksum := cos.NewCksumHash(cos.ChecksumMD5)
	writer := io.MultiWriter(cksum.H, file)
	numBytes, err := io.Copy(writer, r.Body)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	cos.Close(r.Body)
	cksum.Finalize()
	calculatedMD5 := cksum.Value()
	if partMD5 != "" && !cksum.Equal(recvMD5) {
		t.writeErrStatusf(w, r, http.StatusBadRequest,
			"MD5 checksum mismatch: got %s, calculated %s", partMD5, calculatedMD5)
		return
	}

	npart := &s3.MptPart{
		MD5:   cksum.Value(),
		FQN:   workfileFQN,
		Size:  numBytes,
		Ctime: mono.NanoTime(),
		Num:   partNum,
	}
	err = s3.AddPart(uploadID, npart)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	w.Header().Set(cmn.AzCksumHeader, cksum.Value()) // TODO: s3cmd does not use it
	w.Header().Set(cmn.S3CksumHeader, cksum.Value()) // But s3cmd checks this one
}

// Initialize multipart upload.
// - Generate UUID for the upload
// - Return the UUID to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (t *target) startMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	if len(items) < 2 {
		t.writeErrMsg(w, r, "bucket and object names are required to start multipart upload",
			http.StatusBadRequest)
		return
	}
	bckName := items[0]
	objName := strings.Join(items[1:], "/")
	bck, err := newBckFromQ(bckName, q, nil)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.LOM{ObjName: objName}
	err = lom.InitBck(bck.Bucket())
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	uploadID := cos.GenUUID()
	s3.InitUpload(uploadID, objName)
	result := &s3.InitiateMptUploadResult{
		Bucket:   bckName,
		Key:      objName,
		UploadID: uploadID,
	}
	sgl := memsys.PageMM().NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// Complete multipart upload.
// Body contains XML with the list of parts that must be on the storage already.
// 1. Check that all parts from request body present
// 2. Merge all parts into a single file and calculate its ETag
// 3. Return ETag to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
// TODO: lom.Lock; ETag => customMD
func (t *target) completeMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	if len(items) < 2 {
		t.writeErrMsg(w, r, "bucket and object names are required to complete multipart upload",
			http.StatusBadRequest)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		t.writeErrMsg(w, r, "empty uploadId")
		return
	}
	decoder := xml.NewDecoder(r.Body)
	partList := &s3.CompleteMptUpload{}
	if err := decoder.Decode(partList); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if len(partList.Parts) == 0 {
		t.writeErrMsg(w, r, "empty list of upload parts")
		return
	}

	bckName := items[0]
	objName := strings.Join(items[1:], "/")
	bck, err := newBckFromQ(bckName, q, nil)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom := &cluster.LOM{ObjName: objName}
	if err = lom.InitBck(bck.Bucket()); err != nil {
		t.writeErr(w, r, err)
		return
	}

	// do 1. through 7.
	var (
		obj         io.WriteCloser
		objWorkfile string
		objMD5      string
	)
	// 1. sort
	sort.Slice(partList.Parts, func(i, j int) bool {
		return partList.Parts[i].PartNumber < partList.Parts[j].PartNumber
	})
	// 2. check existence and get specified
	nparts, err := s3.CheckParts(uploadID, partList.Parts)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	// 3. cycle through parts and do appending
	buf, slab := t.gmm.Alloc()
	defer slab.Free(buf)
	for _, partInfo := range nparts {
		var err error
		objMD5 += partInfo.MD5
		// first part
		if obj == nil {
			objWorkfile = partInfo.FQN
			obj, err = os.OpenFile(objWorkfile, os.O_APPEND|os.O_WRONLY, cos.PermRWR)
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
			continue
		}
		// 2nd etc. parts
		nextPart, err := os.Open(partInfo.FQN)
		if err != nil {
			cos.Close(obj)
			t.writeErr(w, r, err)
			return
		}
		if _, err := io.CopyBuffer(obj, nextPart, buf); err != nil {
			cos.Close(obj)
			cos.Close(nextPart)
			t.writeErr(w, r, err)
			return
		}
		cos.Close(nextPart)
	}
	cos.Close(obj)

	// 4. md5, size, atime
	eTagMD5 := cos.NewCksumHash(cos.ChecksumMD5)
	_, err = eTagMD5.H.Write([]byte(objMD5)) // Should never fail?
	debug.AssertNoErr(err)
	eTagMD5.Finalize()
	objETag := fmt.Sprintf("%s-%d", eTagMD5.Value(), len(partList.Parts))

	size, err := s3.ObjSize(uploadID)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	lom.SetSize(size)
	lom.SetAtimeUnix(time.Now().UnixNano())

	// 5. finalize
	t.FinalizeObj(lom, objWorkfile, nil)

	// 6. mpt state => xattr
	s3.FinishUpload(uploadID, lom.FQN, false /*aborted*/)

	// 7. respond
	result := &s3.CompleteMptUploadResult{Bucket: bckName, Key: objName, ETag: objETag}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	w.Header().Set(cmn.S3CksumHeader, objETag)
	sgl.WriteTo(w)
	sgl.Free()
}

// List already stored parts of the active multipart upload by bucket name and uploadID.
// NOTE: looks like `s3cmd` lists upload parts before checking if any parts can be skipped.
// s3cmd is OK to receive an empty body in response with status=200. In this
// case s3cmd sends all parts.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (t *target) listMptParts(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, q url.Values) {
	uploadID := q.Get(s3.QparamMptUploadID)
	parts, err := s3.ListParts(uploadID)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	result := &s3.ListPartsResult{Bucket: bck.Name, Key: objName, UploadID: uploadID, Parts: parts}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// List all active multipart uploads for a bucket.
// Body is empty; URL path /bucket with everything else in the query, see:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
// GET /?uploads&delimiter=Delimiter&encoding-type=EncodingType&key-marker=KeyMarker&
//               max-uploads=MaxUploads&prefix=Prefix&upload-id-marker=UploadIdMarker
func (t *target) listMptUploads(w http.ResponseWriter, bck *cluster.Bck, q url.Values) {
	_ = q // TODO: use to support key-marker, upload-id-marker, max-uploads

	result := s3.ListUploads(bck.Name)
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	sgl.WriteTo(w)
	sgl.Free()
}

// Abort an active multipart upload.
// Body is empty, only URL query contains uploadID
// 1. uploadID must exists
// 2. Remove all temporary files
// 3. Remove all info from in-memory structs
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
func (t *target) abortMptUpload(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	if len(items) < 2 {
		t.writeErrMsg(w, r, "bucket and object names are required to complete multipart upload",
			http.StatusBadRequest)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	if !s3.UploadExists(uploadID) {
		t.writeErrStatusf(w, r, http.StatusNotFound, "upload %q does not exist", uploadID)
		return
	}
	s3.FinishUpload(uploadID, "", true /*aborted*/)

	// Respond with status 204(!see the docs) and empty body.
	w.WriteHeader(http.StatusNoContent)
}

// Acts on an already multipart-uploaded object, returns `partNumber` (URL query)
// part of the object.
// The object must have been multipart-uploaded beforehand.
// See:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (t *target) getMptPart(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, q url.Values) {
	// TODO: write code
	partNo := q.Get(s3.QparamMptPartNo)
	_ = partNo

	t.writeErrStatusf(w, r, http.StatusNotImplemented,
		"%s/%s: GET part of the multipart upload is not implemented yet", bck.Name, objName)
}
