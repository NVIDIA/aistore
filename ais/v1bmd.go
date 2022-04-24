// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

// (compare w/ cmn/v1config.go)
func loadBMDV1(path string, bmd *bucketMD) error {
	var old v1BMD
	if _, err := jsp.LoadMeta(path, &old); err != nil {
		return err
	}
	bmd.Version, bmd.UUID, bmd.Ext = old.Version, old.UUID, old.Ext

	// iterate v1 source to copy same-name/same-type fields while taking special care
	// of assorted changes
	for provider, namespaces := range old.Providers {
		for nsUname, buckets := range namespaces {
			for name, props := range buckets {
				ns := cmn.ParseNsUname(nsUname)
				bck := cluster.NewBck(name, provider, ns, &cmn.BucketProps{})
				bck.Props.Provider = props.Provider
				bck.Props.Created = props.Created
				bck.Props.Extra = props.Extra
				bck.Props.BackendBck = props.BackendBck
				bck.Props.BackendBck.Props = nil
				bck.Props.BID = props.BID
				err := cmn.IterFields(props, func(name string, fld cmn.IterField) (error, bool /*stop*/) {
					switch {
					case name == "md_write":
						v, ok := fld.Value().(apc.WritePolicy)
						debug.Assert(ok)
						bck.Props.WritePolicy.MD = v
						return nil, false
					case name == "ec.batch_size", name == "mirror.optimize_put", name == "mirror.util_thresh":
						return nil, false
					case name == "lru.lowwm", name == "lru.highwm", name == "lru.out_of_space":
						return nil, false
					case name == "provider", name == "created", name == "extra", name == "backend_bck":
						return nil, false
					}

					// copy dst = fld.Value()
					return cmn.UpdateFieldValue(bck.Props, name, fld.Value()), false /*stop*/
				}, cmn.IterOpts{OnlyRead: true})
				if err != nil {
					return err
				}

				bmd.Add(bck)
			}
		}
	}
	bmd.cksum = cos.NewCksum(cos.ChecksumXXHash, "dummy-meta-v1") // set temporarily to pass startup equality checks
	return nil
}
