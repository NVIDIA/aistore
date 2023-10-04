// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("API", func() {
	Describe("Apply", func() {
		DescribeTable("should successfully apply all the props",
			func(src cmn.BucketProps, props cmn.BucketPropsToUpdate, expect cmn.BucketProps) {
				src.Apply(&props)
				Expect(src).To(Equal(expect))
			},
			Entry("non-nested field",
				cmn.BucketProps{},
				cmn.BucketPropsToUpdate{
					Access: apc.AccAttrs(1024),
				},
				cmn.BucketProps{
					Access: 1024,
				},
			),
			Entry("non-nested field and non-empty initial struct",
				cmn.BucketProps{
					Provider: apc.AWS,
				},
				cmn.BucketPropsToUpdate{
					Access: apc.AccAttrs(1024),
				},
				cmn.BucketProps{
					Provider: apc.AWS,
					Access:   1024,
				},
			),
			Entry("nested field",
				cmn.BucketProps{},
				cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						Type: apc.String("value"),
					},
				},
				cmn.BucketProps{
					Cksum: cmn.CksumConf{
						Type: "value",
					},
				},
			),
			Entry("multiple nested fields",
				cmn.BucketProps{},
				cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						Type:            apc.String("value"),
						ValidateColdGet: apc.Bool(true),
					},
					EC: &cmn.ECConfToUpdate{
						Enabled:      apc.Bool(true),
						ObjSizeLimit: apc.Int64(1024),
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
			Entry("multiple nested fields and non-empty initial struct",
				cmn.BucketProps{
					Provider: apc.AWS,
					Cksum: cmn.CksumConf{
						ValidateColdGet: true,
						ValidateWarmGet: false,
					},
					Mirror: cmn.MirrorConf{
						Enabled: true,
						Copies:  3,
					},
					LRU: cmn.LRUConf{
						Enabled: true,
					},
				},
				cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						Type: apc.String("value"),
					},
					Mirror: &cmn.MirrorConfToUpdate{
						Enabled: apc.Bool(true),
						Copies:  apc.Int64(3),
					},
					Access: apc.AccAttrs(10),
				},
				cmn.BucketProps{
					Provider: apc.AWS,
					Cksum: cmn.CksumConf{
						Type:            "value",
						ValidateColdGet: true,
						ValidateWarmGet: false,
					},
					Mirror: cmn.MirrorConf{
						Enabled: true,
						Copies:  3,
					},
					LRU: cmn.LRUConf{
						Enabled: true,
					},
					Access: 10,
				},
			),
			Entry("all fields",
				cmn.BucketProps{},
				cmn.BucketPropsToUpdate{
					Versioning: &cmn.VersionConfToUpdate{
						Enabled:         apc.Bool(true),
						ValidateWarmGet: apc.Bool(true),
					},
					Cksum: &cmn.CksumConfToUpdate{
						Type:            apc.String("value"),
						ValidateColdGet: apc.Bool(true),
						ValidateWarmGet: apc.Bool(false),
						ValidateObjMove: apc.Bool(true),
						EnableReadRange: apc.Bool(false),
					},
					Mirror: &cmn.MirrorConfToUpdate{
						Copies:  apc.Int64(10),
						Burst:   apc.Int(32),
						Enabled: apc.Bool(false),
					},
					EC: &cmn.ECConfToUpdate{
						Enabled:      apc.Bool(true),
						ObjSizeLimit: apc.Int64(1024),
						DataSlices:   apc.Int(1024),
						ParitySlices: apc.Int(1024),
						Compression:  apc.String("false"),
					},
					Access: apc.AccAttrs(1024),
					WritePolicy: &cmn.WritePolicyConfToUpdate{
						MD: apc.WPolicy(apc.WriteDelayed),
					},
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
						Copies:  10,
						Burst:   32,
						Enabled: false,
					},
					EC: cmn.ECConf{
						Enabled:      true,
						ObjSizeLimit: 1024,
						DataSlices:   1024,
						ParitySlices: 1024,
						Compression:  "false",
					},
					Access: 1024,
					WritePolicy: cmn.WritePolicyConf{
						Data: "",
						MD:   apc.WriteDelayed,
					},
				},
			),
		)
	})
})
