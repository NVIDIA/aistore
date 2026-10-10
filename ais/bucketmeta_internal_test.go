// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestBucketPropsMergeOCIRegion(t *testing.T) {
	args := bckPropsArgs{
		bck: meta.NewBck("oci-bck", apc.OCI, cmn.NsGlobal),
		hdr: http.Header{
			apc.HdrBackendProvider: []string{apc.OCI},
			apc.HdrOCIRegion:       []string{"us-phoenix-1"},
		},
	}
	props := args.inheritMerge()
	tassert.Errorf(t, props.Extra.OCI.Region == "us-phoenix-1", "unexpected OCI region %q", props.Extra.OCI.Region)
}
