// Package s3compat provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package s3compat

import (
	"encoding/xml"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

// It is 2019-01-01, midnight in UnixNano. Used as creation date
// for buckets that do not have creation date(created with older AIS)
var defaultDate = time.Unix(0, 1546300800000000000)

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
		Owner: BckOwner{ // TODO:
			ID:   "1",
			Name: "ais",
		},
		Buckets: make([]*Bucket, 0),
	}
}

func bckToS3(bck *cluster.Bck) *Bucket {
	created := time.Unix(0, bck.Props.Created)
	if created.Before(defaultDate) {
		// TODO: it is for existing buckets. Their creation time is zero,
		// and AWS CLI complains about "date out of range".
		// Without `Created` AWS CLI complains that field not found.
		created = defaultDate
	}
	return &Bucket{
		Name:    bck.Name,
		Created: created.Format(time.RFC3339),
	}
}

func (r *ListBucketResult) MustMarshal() []byte {
	b, err := xml.Marshal(r)
	cmn.AssertNoErr(err)
	return []byte(xml.Header + string(b))
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

func (r *VersioningConfiguration) MustMarshal() []byte {
	b, err := xml.Marshal(r)
	cmn.AssertNoErr(err)
	return []byte(xml.Header + string(b))
}

func (r *VersioningConfiguration) Enabled() bool {
	return r.Status == versioningEnabled
}
