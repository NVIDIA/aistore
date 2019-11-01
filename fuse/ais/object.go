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
	Size      int64
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

func (obj *Object) Put(data []byte, size uint64) (err error) {
	putArgs := api.PutObjectArgs{
		BaseParams: obj.apiParams,
		Bucket:     obj.bucket,
		Object:     obj.Name,
		Reader:     cmn.NewByteHandle(data),
		Size:       size,
	}

	err = api.PutObject(putArgs)
	if err != nil {
		return newObjectIOError(err, "Put", obj.Name)
	}

	obj.Size = int64(size)
	obj.Atime = time.Now()
	return nil
}
