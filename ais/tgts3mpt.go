// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

const fmtErrBO = "bucket and object names are required to complete multipart upload (have %v)"

// Copy another object or its range as a part of the multipart upload.
// Body is empty, everything in the query params and the header.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
// TODO: not implemented yet
func (*target) putMptCopy(w http.ResponseWriter, r *http.Request, items []string) {
	if len(items) < 2 {
		err := fmt.Errorf(fmtErrBO, items)
		s3.WriteErr(w, r, err, 0)
		return
	}
	s3.WriteErr(w, r, errors.New("not implemented yet"), http.StatusNotImplemented)
}

// PUT a part of the multipart upload.
// Body is empty, everything in the query params and the header.
//
// Obsevation: "Content-MD5" in the parts' headers looks be to be deprecated:
// either not present (s3cmd) or cannot be trusted (aws s3api).
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
func (t *target) putMptPart(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *cluster.Bck) {
	if len(items) < 2 {
		err := fmt.Errorf(fmtErrBO, items)
		s3.WriteErr(w, r, err, 0)
		return
	}
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
	partNum, err := s3.ParsePartNum(part)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if partNum < 1 || partNum > s3.MaxPartsPerUpload {
		err := fmt.Errorf("upload %q: invalid part number %d, must be between 1 and %d",
			uploadID, partNum, s3.MaxPartsPerUpload)
		s3.WriteErr(w, r, err, 0)
		return
	}
	if r.Header.Get(cos.S3HdrObjSrc) != "" {
		s3.WriteErr(w, r, errors.New("uploading a copy is not supported yet"), http.StatusNotImplemented)
		return
	}

	objName := s3.ObjName(items)
	lom := &cluster.LOM{ObjName: objName}
	err = lom.InitBck(bck.Bucket())
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	prefix := fmt.Sprintf("%s.%d", uploadID, partNum) // workfile name format: <upload-id>.<part-number>.<obj-name>
	wfqn := fs.CSM.Gen(lom, fs.WorkfileType, prefix)
	fh, err := os.Create(wfqn)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	var (
		buf, slab = t.gmm.Alloc()
		cksumMD5  = cos.NewCksumHash(cos.ChecksumMD5)
		cksumSHA  *cos.CksumHash
		partSHA   string
		mwriter   io.Writer
	)
	if partSHA = r.Header.Get(cos.S3HdrContentSHA256); partSHA != "" {
		cksumSHA = cos.NewCksumHash(cos.ChecksumSHA256)
		mwriter = io.MultiWriter(cksumMD5.H, cksumSHA.H, fh)
	} else {
		mwriter = io.MultiWriter(cksumMD5.H, fh)
	}
	size, err := io.CopyBuffer(mwriter, r.Body, buf)
	cos.Close(fh)
	slab.Free(buf)
	if err != nil {
		if nerr := cos.RemoveFile(wfqn); nerr != nil {
			glog.Errorf(fmtNested, t, err, "remove", wfqn, nerr)
		}
		s3.WriteErr(w, r, err, 0)
		return
	}
	cksumMD5.Finalize()
	if partSHA != "" {
		cksumSHA.Finalize()
		recvSHA := cos.NewCksum(cos.ChecksumSHA256, partSHA)
		if !cksumSHA.Equal(recvSHA) {
			detail := fmt.Sprintf("upload %q, %s, part %d", uploadID, lom, partNum)
			err = cos.NewBadDataCksumError(&cksumSHA.Cksum, recvSHA, detail)
			s3.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}
	}

	npart := &s3.MptPart{
		MD5:  cksumMD5.Value(),
		FQN:  wfqn,
		Size: size,
		Num:  partNum,
	}
	if err := s3.AddPart(uploadID, npart); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	w.Header().Set(cos.S3CksumHeader, cksumMD5.Value()) // s3cmd checks this one
}

// Initialize multipart upload.
// - Generate UUID for the upload
// - Return the UUID to a caller
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (t *target) startMpt(w http.ResponseWriter, r *http.Request, items []string, bck *cluster.Bck) {
	objName := s3.ObjName(items)
	lom := cluster.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	uploadID := cos.GenUUID()
	s3.InitUpload(uploadID, bck.Name, objName)
	result := &s3.InitiateMptUploadResult{Bucket: bck.Name, Key: objName, UploadID: uploadID}

	sgl := t.gmm.NewSGL(0)
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
func (t *target) completeMpt(w http.ResponseWriter, r *http.Request, items []string, q url.Values, bck *cluster.Bck) {
	uploadID := q.Get(s3.QparamMptUploadID)
	if uploadID == "" {
		s3.WriteErr(w, r, errors.New("empty uploadId"), 0)
		return
	}
	decoder := xml.NewDecoder(r.Body)
	partList := &s3.CompleteMptUpload{}
	if err := decoder.Decode(partList); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	if len(partList.Parts) == 0 {
		s3.WriteErr(w, r, errors.New("empty list of upload parts"), 0)
		return
	}
	objName := s3.ObjName(items)
	lom := &cluster.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
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
		s3.WriteErr(w, r, err, 0)
		return
	}
	// 3. cycle through parts and do appending
	buf, slab := t.gmm.Alloc()
	defer slab.Free(buf)
	for _, partInfo := range nparts {
		objMD5 += partInfo.MD5
		// first part
		if obj == nil {
			objWorkfile = partInfo.FQN
			obj, err = os.OpenFile(objWorkfile, os.O_APPEND|os.O_WRONLY, cos.PermRWR)
			if err != nil {
				s3.WriteErr(w, r, err, 0)
				return
			}
			continue
		}
		// 2nd etc. parts
		nextPart, err := os.Open(partInfo.FQN)
		if err != nil {
			cos.Close(obj)
			s3.WriteErr(w, r, err, 0)
			return
		}
		if _, err := io.CopyBuffer(obj, nextPart, buf); err != nil {
			cos.Close(obj)
			cos.Close(nextPart)
			s3.WriteErr(w, r, err, 0)
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
		s3.WriteErr(w, r, err, 0)
		return
	}
	lom.SetSize(size)
	lom.SetAtimeUnix(time.Now().UnixNano())

	// 5. finalize (note: locks inside)
	t.FinalizeObj(lom, objWorkfile, nil)

	// 6. mpt state => xattr
	exists := s3.FinishUpload(uploadID, lom.FQN, false /*aborted*/)
	debug.Assert(exists)

	// 7. respond
	result := &s3.CompleteMptUploadResult{Bucket: bck.Name, Key: objName, ETag: objETag}
	sgl := t.gmm.NewSGL(0)
	result.MustMarshal(sgl)
	w.Header().Set(cos.HdrContentType, cos.ContentXML)
	w.Header().Set(cos.S3CksumHeader, objETag)
	sgl.WriteTo(w)
	sgl.Free()
}

// List already stored parts of the active multipart upload by bucket name and uploadID.
// (NOTE: `s3cmd` lists upload parts before checking if any parts can be skipped.)
// s3cmd is OK to receive an empty body in response with status=200. In this
// case s3cmd sends all parts.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (t *target) listMptParts(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, q url.Values) {
	uploadID := q.Get(s3.QparamMptUploadID)

	lom := &cluster.LOM{ObjName: objName}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}

	parts, err, errCode := s3.ListParts(uploadID, lom)
	if err != nil {
		s3.WriteErr(w, r, err, errCode)
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
// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
// GET /?uploads&delimiter=Delimiter&encoding-type=EncodingType&key-marker=KeyMarker&
// max-uploads=MaxUploads&prefix=Prefix&upload-id-marker=UploadIdMarker
func (t *target) listMptUploads(w http.ResponseWriter, bck *cluster.Bck, q url.Values) {
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
	result := s3.ListUploads(bck.Name, idMarker, maxUploads)
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
func (*target) abortMptUpload(w http.ResponseWriter, r *http.Request, items []string, q url.Values) {
	if len(items) < 2 {
		err := fmt.Errorf(fmtErrBO, items)
		s3.WriteErr(w, r, err, 0)
		return
	}
	uploadID := q.Get(s3.QparamMptUploadID)
	exists := s3.FinishUpload(uploadID, "", true /*aborted*/)
	if !exists {
		err := fmt.Errorf("upload %q does not exist", uploadID)
		s3.WriteErr(w, r, err, http.StatusNotFound)
		return
	}

	// Respond with status 204(!see the docs) and empty body.
	w.WriteHeader(http.StatusNoContent)
}

// Acts on an already multipart-uploaded object, returns `partNumber` (URL query)
// part of the object.
// The object must have been multipart-uploaded beforehand.
// See:
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (t *target) getMptPart(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, q url.Values) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	partNum, err := s3.ParsePartNum(q.Get(s3.QparamMptPartNo))
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	// load mpt xattr and find out the part num's offset & size
	off, size, status, err := s3.OffsetSorted(lom, partNum)
	if err != nil {
		s3.WriteErr(w, r, err, status)
	}
	fh, err := os.Open(lom.FQN)
	if err != nil {
		s3.WriteErr(w, r, err, 0)
		return
	}
	buf, slab := t.gmm.AllocSize(size)
	reader := io.NewSectionReader(fh, off, size)
	if _, err := io.CopyBuffer(w, reader, buf); err != nil {
		s3.WriteErr(w, r, err, 0)
	}
	cos.Close(fh)
	slab.Free(buf)
}
