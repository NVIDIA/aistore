// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
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

	// Bucket versioning
	VersioningConfiguration struct {
		Status string `xml:"Status"`
	}

	// Multiple object delete request
	Delete struct {
		Quiet  bool                `xml:"Quiet"`
		Object []*DeleteObjectInfo `xml:"Object"`
	}
	DeleteObjectInfo struct {
		Key     string `xml:"Key"`
		Version string `xml:"Version"`
	}
)

func NewListBucketResult() *ListBucketResult {
	return &ListBucketResult{
		Ns: s3Namespace,
		Owner: BckOwner{
			ID:   "1", // NOTE: to satisfy s3 (not used on our side)
			Name: "ais",
		},
		Buckets: make([]*Bucket, 0),
	}
}

func bckToS3(bck *cluster.Bck) *Bucket {
	created := time.Unix(0, bck.Props.Created)
	return &Bucket{
		Name:    bck.Name,
		Created: created.Format(time.RFC3339),
	}
}

func (r *ListBucketResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	cos.AssertNoErr(err)
}

func (r *ListBucketResult) Add(bck *cluster.Bck) {
	r.Buckets = append(r.Buckets, bckToS3(bck))
}

func NewVersioningConfiguration(enabled bool) *VersioningConfiguration {
	if enabled {
		return &VersioningConfiguration{Status: versioningEnabled}
	}
	return &VersioningConfiguration{Status: versioningDisabled}
}

func (r *VersioningConfiguration) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	cos.AssertNoErr(err)
}

func (r *VersioningConfiguration) Enabled() bool {
	return r.Status == versioningEnabled
}
