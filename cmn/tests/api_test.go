// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("API", func() {
	Describe("Apply", func() {
		DescribeTable("should successfully apply all the props",
			func(src cmn.Bprops, props cmn.BpropsToSet, expect cmn.Bprops) {
				src.Apply(&props)
				Expect(src).To(Equal(expect))
			},
			Entry("non-nested field",
				cmn.Bprops{},
				cmn.BpropsToSet{
					Access: apc.Ptr[apc.AccessAttrs](1024),
				},
				cmn.Bprops{
					Access: 1024,
				},
			),
			Entry("non-nested field and non-empty initial struct",
				cmn.Bprops{
					Provider: apc.AWS,
				},
				cmn.BpropsToSet{
					Access: apc.Ptr[apc.AccessAttrs](1024),
				},
				cmn.Bprops{
					Provider: apc.AWS,
					Access:   1024,
				},
			),
			Entry("nested field",
				cmn.Bprops{},
				cmn.BpropsToSet{
					Cksum: &cmn.CksumConfToSet{
						Type: apc.Ptr("value"),
					},
				},
				cmn.Bprops{
					Cksum: cmn.CksumConf{
						Type: "value",
					},
				},
			),
			Entry("multiple nested fields",
				cmn.Bprops{},
				cmn.BpropsToSet{
					Cksum: &cmn.CksumConfToSet{
						Type:            apc.Ptr("value"),
						ValidateColdGet: apc.Ptr(true),
					},
					EC: &cmn.ECConfToSet{
						Enabled:      apc.Ptr(true),
						ObjSizeLimit: apc.Ptr[int64](1024),
					},
				},
				cmn.Bprops{
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
				cmn.Bprops{
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
				cmn.BpropsToSet{
					Cksum: &cmn.CksumConfToSet{
						Type: apc.Ptr("value"),
					},
					Mirror: &cmn.MirrorConfToSet{
						Enabled: apc.Ptr(true),
						Copies:  apc.Ptr[int64](3),
					},
					Access: apc.Ptr[apc.AccessAttrs](10),
				},
				cmn.Bprops{
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
				cmn.Bprops{},
				cmn.BpropsToSet{
					Versioning: &cmn.VersionConfToSet{
						Enabled:         apc.Ptr(true),
						ValidateWarmGet: apc.Ptr(true),
					},
					Cksum: &cmn.CksumConfToSet{
						Type:            apc.Ptr("value"),
						ValidateColdGet: apc.Ptr(true),
						ValidateWarmGet: apc.Ptr(false),
						ValidateObjMove: apc.Ptr(true),
						EnableReadRange: apc.Ptr(false),
					},
					Mirror: &cmn.MirrorConfToSet{
						Copies:  apc.Ptr[int64](10),
						Burst:   apc.Ptr(32),
						Enabled: apc.Ptr(false),
					},
					EC: &cmn.ECConfToSet{
						XactConfToSet: cmn.XactConfToSet{
							Compression: apc.Ptr(apc.CompressNever),
						},
						Enabled:      apc.Ptr(true),
						ObjSizeLimit: apc.Ptr[int64](1024),
						DataSlices:   apc.Ptr(1024),
						ParitySlices: apc.Ptr(1024),
					},
					Access: apc.Ptr[apc.AccessAttrs](1024),
					WritePolicy: &cmn.WritePolicyConfToSet{
						MD: apc.Ptr(apc.WriteDelayed),
					},
				},
				cmn.Bprops{
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
						XactConf: cmn.XactConf{
							Compression: apc.CompressNever,
						},
						Enabled:      true,
						ObjSizeLimit: 1024,
						DataSlices:   1024,
						ParitySlices: 1024,
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
