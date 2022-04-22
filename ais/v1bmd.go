// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// backward compatibility, to support BMD meta-v1 => v2 upgrades
// NOTE: to be removed in 3.11
// See also: cmn/v1config.go

const v1MetaverBMD = 1

type (
	v1BMD struct {
		Version   int64       `json:"version,string"`
		UUID      string      `json:"uuid"`
		Providers v1Providers `json:"providers"`
		Ext       interface{} `json:"ext,omitempty"`
	}

	v1Buckets    map[string]*cmn.V1BucketProps
	v1Namespaces map[string]v1Buckets
	v1Providers  map[string]v1Namespaces
)

// interface guard
var _ jsp.Opts = (*v1BMD)(nil)

var v1BmdJspOpts = jsp.CCSign(v1MetaverBMD)

func (*v1BMD) JspOpts() jsp.Options { return v1BmdJspOpts }

func loadBMDV1(path string, bmd *bucketMD) error {
	var old v1BMD
	if _, err := jsp.LoadMeta(path, &old); err != nil {
		return err
	}
	bmd.Version, bmd.UUID, bmd.Ext = old.Version, old.UUID, old.Ext
	for provider, namespaces := range old.Providers {
		for nsUname, buckets := range namespaces {
			for name, props := range buckets {
				ns := cmn.ParseNsUname(nsUname)
				bck := cluster.NewBck(name, provider, ns, &cmn.BucketProps{})

				bck.Props.Provider = props.Provider
				bck.Props.BackendBck = props.BackendBck
				bck.Props.Versioning = props.Versioning
				bck.Props.Cksum = props.Cksum
				bck.Props.Mirror = props.Mirror
				bck.Props.Access = props.Access
				bck.Props.Extra = props.Extra
				bck.Props.BID = props.BID
				bck.Props.Created = props.Created
				bck.Props.Renamed = props.Renamed

				// changed in v2
				bck.Props.WritePolicy.MD = props.MDWrite
				bck.Props.LRU.DontEvictTime = props.LRU.DontEvictTime
				bck.Props.LRU.CapacityUpdTime = props.LRU.CapacityUpdTime
				bck.Props.LRU.Enabled = props.LRU.Enabled
				bck.Props.EC.ObjSizeLimit = props.EC.ObjSizeLimit
				bck.Props.EC.Compression = props.EC.Compression
				bck.Props.EC.DataSlices = props.EC.DataSlices
				bck.Props.EC.ParitySlices = props.EC.ParitySlices
				bck.Props.EC.Enabled = props.EC.Enabled
				bck.Props.EC.DiskOnly = props.EC.DiskOnly

				bmd.Add(bck)
			}
		}
	}
	bmd.cksum = cos.NewCksum(cos.ChecksumXXHash, "dummy-meta-v1")
	return nil
}
