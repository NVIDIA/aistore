// Package ais implements an AIStore client.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

type Bucket struct {
	name      string
	apiParams api.BaseParams
}

func NewBucket(name string, apiParams api.BaseParams) *Bucket {
	return &Bucket{
		name:      name,
		apiParams: apiParams,
	}
}

func (bck *Bucket) HeadObject(objName string) (object *Object, err error) {
	obj, err := api.HeadObject(bck.apiParams, bck.name, "", objName)
	if err != nil {
		return nil, newBucketIOError(err, "HeadObject", objName)
	}

	object = &Object{
		apiParams: bck.apiParams,
		bucket:    bck.name,
		Name:      objName,
		Size:      obj.Size,
		Atime:     obj.Atime,
	}
	return
}

func (bck *Bucket) ListObjects(prefix string) (objs []*Object, err error) {
	selectMsg := &cmn.SelectMsg{
		Prefix: prefix,
		Props:  cmn.GetPropsSize,
	}
	listResult, err := api.ListBucketFast(bck.apiParams, bck.name, selectMsg)
	if err != nil {
		return nil, newBucketIOError(err, "ListObjects")
	}

	objs = make([]*Object, 0, len(listResult.Entries))
	for _, obj := range listResult.Entries {
		objs = append(objs, NewObject(obj.Name, bck, obj.Size))
	}
	return
}

func (bck *Bucket) DeleteObject(objName string) (err error) {
	err = api.DeleteObject(bck.apiParams, bck.name, objName, "")
	if err != nil {
		err = newBucketIOError(err, "DeleteObject", objName)
	}
	return
}
