// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

type Object struct {
	apiParams api.BaseParams // FIXME: it is quite a big struct and should be removed
	bck       cmn.Bck        // FIXME: bucket name is static so we should not have it as a field
	Name      string
	Size      int64
	Atime     time.Time
}

func NewObject(objName string, bucket Bucket, sizes ...int64) *Object {
	var size int64
	if len(sizes) > 0 {
		size = sizes[0]
	}

	return &Object{
		apiParams: bucket.APIParams(),
		bck:       bucket.Bck(),
		Name:      objName,
		Size:      size,
		Atime:     time.Now(),
	}
}

func (obj *Object) Put(r cmn.ReadOpenCloser) (err error) {
	putArgs := api.PutObjectArgs{
		BaseParams: obj.apiParams,
		Bck:        obj.bck,
		Object:     obj.Name,
		Reader:     r,
	}
	err = api.PutObject(putArgs)
	if err != nil {
		err = newObjectIOError(err, "Put", obj.Name)
	}
	return
}

func (obj *Object) GetChunk(w io.Writer, offset, length int64) (n int64, err error) {
	hdr := cmn.AddRangeToHdr(nil, offset, length)
	objArgs := api.GetObjectInput{
		Writer: w,
		Header: hdr,
	}

	n, err = api.GetObject(obj.apiParams, obj.bck, obj.Name, objArgs)
	if err != nil {
		return 0, newObjectIOError(err, "GetChunk", obj.Name)
	}
	return
}

func (obj *Object) Append(r cmn.ReadOpenCloser, prevHandle string, size int64) (handle string, err error) {
	appendArgs := api.AppendArgs{
		BaseParams: obj.apiParams,
		Bck:        obj.bck,
		Object:     obj.Name,
		Handle:     prevHandle,
		Reader:     r,
		Size:       size,
	}
	handle, err = api.AppendObject(appendArgs)
	if err != nil {
		return handle, newObjectIOError(err, "Append", obj.Name)
	}
	return handle, nil
}

func (obj *Object) Flush(handle string) (err error) {
	flushArgs := api.FlushArgs{
		BaseParams: obj.apiParams,
		Bck:        obj.bck,
		Object:     obj.Name,
		Handle:     handle,
	}
	if err = api.FlushObject(flushArgs); err != nil {
		return newObjectIOError(err, "Flush", obj.Name)
	}
	return nil
}
