// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

import (
	"encoding/xml"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	// List bucket response
	ListBucketResult struct {
		Ns      string    `xml:"xmlns,attr"`
		Owner   BckOwner  `xml:"Owner"`
		Buckets []*Bucket `xml:"Buckets>Bucket"`
	}
	BckOwner struct {
		ID   string `xml:"ID"`
		Name string `xml:"DisplayName"`
	}
	Bucket struct {
		Name    string `xml:"Name"`
		Created string `xml:"CreationDate"`
	}
)

func NewListBucketResult() *ListBucketResult {
	return &ListBucketResult{
		Ns: s3Namespace,
		Owner: BckOwner{ // TODO:
			ID:   "1",
			Name: "ais",
		},
		Buckets: make([]*Bucket, 0),
	}
}

func bckToS3(bck *cmn.Bck) *Bucket {
	return &Bucket{
		Name:    bck.Name,
		Created: time.Now().UTC().Format(time.RFC3339), // TODO:
	}
}

func (r *ListBucketResult) MustMarshal() []byte {
	b, err := xml.Marshal(r)
	cmn.AssertNoErr(err)
	return []byte(xml.Header + string(b))
}

func (r *ListBucketResult) Add(bck *cmn.Bck) {
	r.Buckets = append(r.Buckets, bckToS3(bck))
}
