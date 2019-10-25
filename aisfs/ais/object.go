// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
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

	Name  string
	Size  int64
	Atime time.Time
}

func (obj *Object) DownloadPart(offset int64, length int) (io.Reader, int64, error) {
	query := url.Values{}
	query.Add(cmn.URLParamOffset, strconv.Itoa(int(offset)))
	query.Add(cmn.URLParamLength, strconv.Itoa(length))

	buffer := bytes.NewBuffer(make([]byte, 0, length))

	objArgs := api.GetObjectInput{
		Writer: buffer,
		Query:  query,
	}
	n, err := api.GetObject(obj.apiParams, obj.bucket, obj.Name, objArgs)
	if err != nil {
		return nil, 0, newObjectIOError(err, "DownloadPart", obj.Name)
	}

	return buffer, n, nil
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
