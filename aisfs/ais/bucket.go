// Package ais implements an AIStore client.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	httpTransportTimeout = 60 * time.Second  // FIXME: Too long?
	httpClientTimeout    = 300 * time.Second // FIXME: Too long?
	requestObjectProps   = cmn.GetPropsSize + "," + cmn.GetPropsAtime
)

func apiParams(clusterURL string) *api.BaseParams {
	httpTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: httpTransportTimeout,
		}).DialContext,
	}

	httpClient := &http.Client{
		Timeout:   httpClientTimeout,
		Transport: httpTransport,
	}

	return &api.BaseParams{
		Client: httpClient,
		URL:    clusterURL,
	}
}

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

func (bck *Bucket) HeadObject(name string) (object *Object, err error) {
	obj, err := api.HeadObject(bck.apiParams, bck.name, "", name)
	if err != nil {
		return
	}

	object = &Object{
		apiParams: apiParams(bck.apiParams.URL),
		bucket:    bck.name,
		Name:      name,
		Size:      obj.Size,
		Atime:     obj.Atime,
	}

	return
}

func (bck *Bucket) ListObjects(prefix string, limit int) (objlist []*Object, err error) {
	selectMsg := &cmn.SelectMsg{
		Props:  requestObjectProps,
		Prefix: prefix,
	}

	listResult, err := api.ListBucket(bck.apiParams, bck.name, selectMsg, limit)
	if err != nil {
		return
	}

	objlist = make([]*Object, 0, len(listResult.Entries))
	for _, entry := range listResult.Entries {
		var (
			atime  time.Time
			object *Object
		)
		atime, _ = time.Parse(time.RFC822, entry.Atime) // FIXME: Handle possible error
		object = &Object{
			apiParams: apiParams(bck.apiParams.URL),
			bucket:    bck.name,
			Name:      entry.Name,
			Size:      entry.Size,
			Atime:     atime,
		}
		objlist = append(objlist, object)
	}

	return
}

func (bck *Bucket) ListObjectNames(prefix string) (names []string, err error) {
	selectMsg := &cmn.SelectMsg{
		Prefix: prefix,
		Fast:   true,
	}

	listResult, err := api.ListBucketFast(bck.apiParams, bck.name, selectMsg)
	if err != nil {
		return
	}

	names = make([]string, 0, len(listResult.Entries))
	for _, object := range listResult.Entries {
		names = append(names, object.Name)
	}

	return
}

func (bck *Bucket) DeleteObject(objName string) error {
	return api.DeleteObject(bck.apiParams, bck.name, objName, "")
}
