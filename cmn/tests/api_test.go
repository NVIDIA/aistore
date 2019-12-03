// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("API", func() {
	Describe("Apply", func() {
		DescribeTable("should successfully apply all the props",
			func(props cmn.BucketPropsToUpdate, expect cmn.BucketProps) {
				src := cmn.BucketProps{}
				src.Apply(props)
				Expect(src).To(Equal(expect))
			},
			Entry("non-nested field",
				cmn.BucketPropsToUpdate{
					AccessAttrs: api.Uint64(1024),
				},
				cmn.BucketProps{
					AccessAttrs: 1024,
				},
			),
			Entry("nested field",
				cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						Type: api.String("value"),
					},
				},
				cmn.BucketProps{
					Cksum: cmn.CksumConf{
						Type: "value",
					},
				},
			),
			Entry("multiple nested field",
				cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						Type:            api.String("value"),
						ValidateColdGet: api.Bool(true),
					},
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(1024),
					},
				},
				cmn.BucketProps{
					Cksum: cmn.CksumConf{
						Type:            "value",
						ValidateColdGet: true,
						ValidateWarmGet: false, // check default value didn't change
					},
					EC: cmn.ECConf{
						Enabled:      true,
						ObjSizeLimit: 1024,
						DataSlices:   0, // check default value didn't change
						ParitySlices: 0, // check default value didn't change
					},
				},
			),
			Entry("all fields",
				cmn.BucketPropsToUpdate{
					Versioning: &cmn.VersionConfToUpdate{
						Enabled:         api.Bool(true),
						ValidateWarmGet: api.Bool(true),
					},
					Cksum: &cmn.CksumConfToUpdate{
						Type:            api.String("value"),
						ValidateColdGet: api.Bool(true),
						ValidateWarmGet: api.Bool(false),
						ValidateObjMove: api.Bool(true),
						EnableReadRange: api.Bool(false),
					},
					Mirror: &cmn.MirrorConfToUpdate{
						Copies:      api.Int64(10),
						Burst:       api.Int64(32),
						UtilThresh:  api.Int64(64),
						OptimizePUT: api.Bool(true),
						Enabled:     api.Bool(false),
					},
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ObjSizeLimit: api.Int64(1024),
						DataSlices:   api.Int(1024),
						ParitySlices: api.Int(1024),
						Compression:  api.String("false"),
					},
					AccessAttrs: api.Uint64(1024),
				},
				cmn.BucketProps{
					Versioning: cmn.VersionConf{
						Enabled:         true,
						ValidateWarmGet: true,
					},
					Cksum: cmn.CksumConf{
						Type:            "value",
						ValidateColdGet: true,
						ValidateWarmGet: false,
						ValidateObjMove: true,
						EnableReadRange: false,
					},
					Mirror: cmn.MirrorConf{
						Copies:      10,
						Burst:       32,
						UtilThresh:  64,
						OptimizePUT: true,
						Enabled:     false,
					},
					EC: cmn.ECConf{
						Enabled:      true,
						ObjSizeLimit: 1024,
						DataSlices:   1024,
						ParitySlices: 1024,
						Compression:  "false",
					},
					AccessAttrs: 1024,
				},
			),
		)
	})
})
