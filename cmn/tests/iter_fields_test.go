// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("IterFields", func() {
	type (
		foo struct {
			Foo int `list:"omit"`
			Bar int `json:"bar"`
		}
	)

	Describe("IterFields", func() {
		DescribeTable("should successfully iterate fields in structs",
			func(v interface{}, expected map[string]interface{}) {
				got := make(map[string]interface{})
				err := cmn.IterFields(v, func(tag string, field cmn.IterField) (error, bool) {
					got[tag] = field.Value()
					return nil, false
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(expected))
			},
			Entry("list bucket props",
				cmn.BucketProps{
					CloudProvider: cmn.ProviderAIS,
					EC: cmn.ECConf{
						Enabled:      true,
						ParitySlices: 1024,
					},
					LRU: cmn.LRUConf{},
					Cksum: cmn.CksumConf{
						Type: cmn.PropInherit,
					},
				},
				map[string]interface{}{
					"cloud_provider": cmn.ProviderAIS,

					"mirror.enabled":      false,
					"mirror.copies":       int64(0),
					"mirror.util_thresh":  int64(0),
					"mirror.burst_buffer": int64(0),
					"mirror.optimize_put": false,

					"ec.enabled":       true,
					"ec.parity_slices": 1024,
					"ec.data_slices":   0,
					"ec.objsize_limit": int64(0),
					"ec.compression":   "",

					"versioning.enabled":           false,
					"versioning.validate_warm_get": false,

					"cksum.type":              cmn.PropInherit,
					"cksum.validate_warm_get": false,
					"cksum.validate_cold_get": false,
					"cksum.validate_obj_move": false,
					"cksum.enable_read_range": false,

					"lru.enabled":           false,
					"lru.lowwm":             int64(0),
					"lru.highwm":            int64(0),
					"lru.out_of_space":      int64(0),
					"lru.dont_evict_time":   "",
					"lru.capacity_upd_time": "",

					"aattrs": uint64(0),
					"bid":    uint64(0),
				},
			),
			Entry("check for omit tag",
				foo{Foo: 1, Bar: 2},
				map[string]interface{}{
					"bar": 2,
				},
			),
		)
	})

	Describe("UpdateFieldValue", func() {
		DescribeTable("should successfully update the fields in struct",
			func(v interface{}, values map[string]interface{}, expected interface{}) {
				for name, value := range values {
					err := cmn.UpdateFieldValue(v, name, value)
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(v).To(Equal(expected))
			},
			Entry("update some bucket props",
				&cmn.BucketProps{},
				map[string]interface{}{
					"mirror.enabled":      "true", // type == bool
					"mirror.copies":       "120",  // type == int
					"mirror.util_thresh":  int64(0),
					"mirror.burst_buffer": "9560", // type == int64
					"mirror.optimize_put": false,

					"ec.enabled":       true,
					"ec.parity_slices": 1024,
					"ec.objsize_limit": int64(0),
					"ec.compression":   "",

					"versioning.enabled": false,

					"cksum.type": cmn.PropInherit,

					"aattrs": "12", // type == uint64
				},
				&cmn.BucketProps{
					Mirror: cmn.MirrorConf{
						Enabled: true,
						Copies:  120,
						Burst:   9560,
					},
					EC: cmn.ECConf{
						Enabled:      true,
						ParitySlices: 1024,
					},
					LRU: cmn.LRUConf{},
					Cksum: cmn.CksumConf{
						Type: cmn.PropInherit,
					},
					AccessAttrs: 12,
				},
			),
		)

		DescribeTable("should error on update",
			func(v interface{}, values map[string]interface{}) {
				for name, value := range values {
					err := cmn.UpdateFieldValue(v, name, value)
					Expect(err).To(HaveOccurred())
				}
			},
			Entry("non-pointer struct", cmn.BucketProps{}, map[string]interface{}{
				"mirror.enabled": true,
			}),
			Entry("readonly field", &cmn.BucketProps{}, map[string]interface{}{
				"cloud_provider": cmn.ProviderAIS,
			}),
			Entry("field not found", &foo{}, map[string]interface{}{
				"foo.bar": 2,
			}),
		)
	})
})
