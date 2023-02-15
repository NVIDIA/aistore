// Package ais implements an AIStore client.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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

func (obj *Object) Bck() cmn.Bck { return obj.bck }

func (obj *Object) Put(r cos.ReadOpenCloser) (err error) {
	putArgs := api.PutArgs{
		BaseParams: obj.apiParams,
		Bck:        obj.bck,
		ObjName:    obj.Name,
		Reader:     r,
	}
	_, err = api.PutObject(putArgs)
	if err != nil {
		err = newObjectIOError(err, "Put", obj.Name)
	}
	return
}

func (obj *Object) GetChunk(w io.Writer, offset, length int64) (n int64, err error) {
	var (
		oah     api.ObjAttrs
		hdr     = cmn.MakeRangeHdr(offset, length)
		getArgs = api.GetArgs{Writer: w, Header: hdr}
	)
	oah, err = api.GetObject(obj.apiParams, obj.bck, obj.Name, &getArgs)
	if err != nil {
		return 0, newObjectIOError(err, "GetChunk", obj.Name)
	}
	n = oah.Size()
	return
}

func (obj *Object) Append(r cos.ReadOpenCloser, prevHandle string, size int64) (handle string, err error) {
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

func (obj *Object) Flush(handle string, cksum *cos.Cksum) (err error) {
	flushArgs := api.FlushArgs{
		BaseParams: obj.apiParams,
		Bck:        obj.bck,
		Object:     obj.Name,
		Handle:     handle,
		Cksum:      cksum,
	}
	if err = api.FlushObject(flushArgs); err != nil {
		return newObjectIOError(err, "Flush", obj.Name)
	}
	return nil
}
