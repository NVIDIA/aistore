// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"encoding/xml"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		String  string `xml:"String"`
	}

	// Bucket versioning
	VersioningConfiguration struct {
		Status string `xml:"Status"`
	}

	// Multiple object delete request
	Delete struct {
		Object []*DeleteObjectInfo `xml:"Object"`
		Quiet  bool                `xml:"Quiet"`
	}
	DeleteObjectInfo struct {
		Key     string `xml:"Key"`
		Version string `xml:"Version"`
	}
)

func NewListBucketResult() (r *ListBucketResult) {
	r = &ListBucketResult{
		Ns:      s3Namespace,
		Owner:   BckOwner{Name: "ListAllMyBucketsResult"},
		Buckets: make([]*Bucket, 0, 8),
	}
	r.Owner.ID = "1" // to satisfy s3
	return
}

func (r *ListBucketResult) Add(bck *cluster.Bck) {
	var warn string
	for _, b := range r.Buckets {
		if b.Name == bck.Name {
			// NOTE: when bck.Name is not unique
			warn = fmt.Sprintf(" (WARNING: {%s, %s} and {%s, Provider: %s} share the same name)",
				b.Name, b.String, bck.Name, bck.Provider)
		}
	}
	b := &Bucket{
		Name:    bck.Name,
		Created: cos.FormatTime(time.Unix(0, bck.Props.Created), cos.ISO8601),
		String:  "Provider: " + bck.Provider + warn,
	}
	r.Buckets = append(r.Buckets, b)
}

func (r *ListBucketResult) MustMarshal(sgl *memsys.SGL) {
	sgl.Write([]byte(xml.Header))
	err := xml.NewEncoder(sgl).Encode(r)
	debug.AssertNoErr(err)
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
	debug.AssertNoErr(err)
}

func (r *VersioningConfiguration) Enabled() bool {
	return r.Status == versioningEnabled
}
