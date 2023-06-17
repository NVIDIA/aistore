// Package ais implements an AIStore client.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	Bucket interface {
		Name() string
		APIParams() api.BaseParams
		Bck() cmn.Bck
		HeadObject(objName string) (obj *Object, exists bool, err error)
		ListObjects(prefix, token string, pageSize uint) (objs []*Object, nextToken string, err error)
		DeleteObject(objName string) (err error)
	}

	bucketAPI struct {
		bck       cmn.Bck
		apiParams api.BaseParams
	}
)

func NewBucket(name string, apiParams api.BaseParams) (bck Bucket, err error) {
	b := cmn.Bck{Name: name}
	b.Props, err = api.HeadBucket(apiParams, b, false /*don't add*/)
	if err != nil {
		return &bucketAPI{bck: b, apiParams: apiParams}, err
	}
	return &bucketAPI{bck: b, apiParams: apiParams}, nil
}

func (bck *bucketAPI) Name() string              { return bck.bck.Name }
func (bck *bucketAPI) Bck() cmn.Bck              { return bck.bck }
func (bck *bucketAPI) APIParams() api.BaseParams { return bck.apiParams }

func (bck *bucketAPI) HeadObject(objName string) (obj *Object, exists bool, err error) {
	objProps, err := api.HeadObject(bck.apiParams, bck.Bck(), objName, 0 /*fltPresence*/)
	if err != nil {
		if herr := cmn.Err2HTTPErr(err); herr != nil && herr.Status == http.StatusNotFound {
			return nil, false, nil
		}
		return nil, false, newBucketIOError(err, "HeadObject")
	}

	return &Object{
		apiParams: bck.apiParams,
		bck:       bck.Bck(),
		Name:      objName,
		Size:      objProps.Size,
		Atime:     time.Unix(0, objProps.Atime),
	}, true, nil
}

func (bck *bucketAPI) ListObjects(prefix, token string, pageSize uint) (objs []*Object, nextToken string, err error) {
	selectMsg := &apc.LsoMsg{
		Prefix:            prefix,
		Props:             apc.GetPropsSize,
		PageSize:          pageSize,
		ContinuationToken: token,
	}
	listResult, err := api.ListObjects(bck.apiParams, bck.Bck(), selectMsg, api.ListArgs{})
	if err != nil {
		return nil, "", newBucketIOError(err, "ListObjects")
	}

	objs = make([]*Object, 0, len(listResult.Entries))
	for _, obj := range listResult.Entries {
		objs = append(objs, NewObject(obj.Name, bck, obj.Size))
	}
	nextToken = listResult.ContinuationToken
	return
}

func (bck *bucketAPI) DeleteObject(objName string) (err error) {
	err = api.DeleteObject(bck.apiParams, bck.Bck(), objName)
	if err != nil {
		err = newBucketIOError(err, "DeleteObject", objName)
	}
	return
}
