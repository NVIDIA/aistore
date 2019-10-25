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
	apiParams *api.BaseParams
	name      string
}

func OpenBucket(clusterURL string, name string) *Bucket {
	return &Bucket{
		apiParams: apiParams(clusterURL),
		name:      name,
	}
}

func (bck *Bucket) NewEmptyObject(objName string) (object *Object, err error) {
	putArgs := api.PutObjectArgs{
		BaseParams: bck.apiParams,
		Bucket:     bck.name,
		Object:     objName,
		Reader:     emptyBuffer(),
	}

	err = api.PutObject(putArgs)
	if err != nil {
		return nil, newBucketIOError(err, "NewEmptyObject", objName)
	}

	return bck.HeadObject(objName)
}

func (bck *Bucket) HeadObject(objName string) (object *Object, err error) {
	obj, err := api.HeadObject(bck.apiParams, bck.name, "", objName)
	if err != nil {
		return nil, newBucketIOError(err, "HeadObject", objName)
	}

	object = &Object{
		apiParams: apiParams(bck.apiParams.URL),
		bucket:    bck.name,
		Name:      objName,
		Size:      obj.Size,
		Atime:     obj.Atime,
	}
	return
}

// HasObjectWithPrefix checks if object with given prefix exists in a bucket.
func (bck *Bucket) HasObjectWithPrefix(prefix string) (exists bool, err error) {
	list, err := api.ListBucket(bck.apiParams, bck.name, &cmn.SelectMsg{Prefix: prefix}, 1)
	if err != nil {
		return false, newBucketIOError(err, "HasObjectWithPrefix")
	}
	return len(list.Entries) > 0, nil
}

func (bck *Bucket) ListObjectNames(prefix string) (names []string, err error) {
	selectMsg := &cmn.SelectMsg{
		Prefix: prefix,
		Fast:   true,
	}

	listResult, err := api.ListBucketFast(bck.apiParams, bck.name, selectMsg)
	if err != nil {
		return nil, newBucketIOError(err, "ListObjectNames")
	}

	names = make([]string, 0, len(listResult.Entries))
	for _, object := range listResult.Entries {
		names = append(names, object.Name)
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
