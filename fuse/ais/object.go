// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

type Object struct {
	apiParams *api.BaseParams
	bucket    string
	Name      string
	Size      uint64
	Atime     time.Time
}

func (obj *Object) GetChunk(w io.Writer, offset int64, length int64) (n int64, err error) {
	query := url.Values{}
	query.Add(cmn.URLParamOffset, strconv.FormatInt(offset, 10))
	query.Add(cmn.URLParamLength, strconv.FormatInt(length, 10))
	objArgs := api.GetObjectInput{
		Writer: w,
		Query:  query,
	}

	n, err = api.GetObject(obj.apiParams, obj.bucket, obj.Name, objArgs)
	if err != nil {
		return 0, newObjectIOError(err, "GetChunk", obj.Name)
	}
	return
}

func (obj *Object) Append(data []byte, prevHandle string) (handle string, err error) {
	appendArgs := api.AppendArgs{
		BaseParams: obj.apiParams,
		Bucket:     obj.bucket,
		Object:     obj.Name,
		Handle:     prevHandle,
		Body:       data,
	}
	handle, err = api.AppendObject(appendArgs)
	if err != nil {
		return handle, newObjectIOError(err, "Append", obj.Name)
	}

	obj.Size += uint64(len(data))
	return handle, nil
}

func (obj *Object) Flush(handle string) (err error) {
	appendArgs := api.AppendArgs{
		BaseParams: obj.apiParams,
		Bucket:     obj.bucket,
		Object:     obj.Name,
		Handle:     handle,
	}
	if err = api.FlushObject(appendArgs); err != nil {
		return newObjectIOError(err, "Flush", obj.Name)
	}
	obj.Atime = time.Now()
	return nil
}
