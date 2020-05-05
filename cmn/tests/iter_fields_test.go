// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("IterFields", func() {
	type (
		Foo struct {
			A int `list:"omit"`
			B int `json:"b"`
		}
		bar struct {
			Foo Foo    `json:"foo"`
			C   string `json:"c"`
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
			Entry("list BucketProps fields",
				cmn.BucketProps{
					Provider: cmn.ProviderAIS,
					OriginBck: cmn.Bck{
						Name:     "name",
						Provider: cmn.ProviderGoogle,
					},
					EC: cmn.ECConf{
						Enabled:      true,
						ParitySlices: 1024,
						BatchSize:    32,
					},
					LRU: cmn.LRUConf{},
					Cksum: cmn.CksumConf{
						Type: cmn.PropInherit,
					},
				},
				map[string]interface{}{
					"provider": cmn.ProviderAIS,

					"origin_bck.name":     "name",
					"origin_bck.provider": cmn.ProviderGoogle,

					"mirror.enabled":      false,
					"mirror.copies":       int64(0),
					"mirror.util_thresh":  int64(0),
					"mirror.burst_buffer": int64(0),
					"mirror.optimize_put": false,

					"ec.enabled":       true,
					"ec.parity_slices": 1024,
					"ec.data_slices":   0,
					"ec.batch_size":    32,
					"ec.objsize_limit": int64(0),
					"ec.compression":   "",

					"versioning.enabled":           false,
					"versioning.validate_warm_get": false,

					"checksum.type":              cmn.PropInherit,
					"checksum.validate_warm_get": false,
					"checksum.validate_cold_get": false,
					"checksum.validate_obj_move": false,
					"checksum.enable_read_range": false,

					"lru.enabled":           false,
					"lru.lowwm":             int64(0),
					"lru.highwm":            int64(0),
					"lru.out_of_space":      int64(0),
					"lru.dont_evict_time":   "",
					"lru.capacity_upd_time": "",

					"access":  uint64(0),
					"created": int64(0),
				},
			),
			Entry("list BucketPropsToUpdate fields",
				&cmn.BucketPropsToUpdate{
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ParitySlices: api.Int(1024),
					},
					LRU: &cmn.LRUConfToUpdate{},
					Cksum: &cmn.CksumConfToUpdate{
						Type: api.String(cmn.PropInherit),
					},
					AccessAttrs: api.Uint64(1024),
				},
				map[string]interface{}{
					"origin_bck.name":     (*string)(nil),
					"origin_bck.provider": (*string)(nil),

					"mirror.enabled":      (*bool)(nil),
					"mirror.copies":       (*int64)(nil),
					"mirror.util_thresh":  (*int64)(nil),
					"mirror.burst_buffer": (*int64)(nil),
					"mirror.optimize_put": (*bool)(nil),

					"ec.enabled":       api.Bool(true),
					"ec.parity_slices": api.Int(1024),
					"ec.data_slices":   (*int)(nil),
					"ec.objsize_limit": (*int64)(nil),
					"ec.compression":   (*string)(nil),

					"versioning.enabled":           (*bool)(nil),
					"versioning.validate_warm_get": (*bool)(nil),

					"checksum.type":              api.String(cmn.PropInherit),
					"checksum.validate_warm_get": (*bool)(nil),
					"checksum.validate_cold_get": (*bool)(nil),
					"checksum.validate_obj_move": (*bool)(nil),
					"checksum.enable_read_range": (*bool)(nil),

					"lru.enabled":      (*bool)(nil),
					"lru.lowwm":        (*int64)(nil),
					"lru.highwm":       (*int64)(nil),
					"lru.out_of_space": (*int64)(nil),

					"access": api.Uint64(1024),
				},
			),
			Entry("check for omit tag",
				Foo{A: 1, B: 2},
				map[string]interface{}{
					"b": 2,
				},
			),
		)

		It("list all the fields (not only leafs)", func() {
			v := bar{Foo: Foo{A: 3, B: 10}, C: "string"}
			expected := map[string]interface{}{
				"foo.b": 10,
				"foo":   Foo{A: 3, B: 10},
				"c":     "string",
			}

			got := make(map[string]interface{})
			err := cmn.IterFields(v, func(tag string, field cmn.IterField) (error, bool) {
				got[tag] = field.Value()
				return nil, false
			}, cmn.IterOpts{VisitAll: true})
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
		})
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
			Entry("update some BucketProps",
				&cmn.BucketProps{
					Versioning: cmn.VersionConf{
						ValidateWarmGet: true,
					},
				},
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

					"checksum.type": cmn.PropInherit,

					"access": "12", // type == uint64
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
					Versioning: cmn.VersionConf{
						Enabled:         false,
						ValidateWarmGet: true,
					},
					AccessAttrs: 12,
				},
			),
			Entry("update some BucketPropsToUpdate",
				&cmn.BucketPropsToUpdate{
					Cksum: &cmn.CksumConfToUpdate{
						ValidateWarmGet: api.Bool(true),
					},
				},
				map[string]interface{}{
					"mirror.enabled":      "true", // type == bool
					"mirror.copies":       "120",  // type == int
					"mirror.burst_buffer": "9560", // type == int64

					"ec.enabled":       true,
					"ec.parity_slices": 1024,
					"ec.objsize_limit": int64(0),
					"ec.compression":   "",

					"versioning.enabled": false,

					"checksum.type": cmn.PropInherit,

					"access": "12", // type == uint64
				},
				&cmn.BucketPropsToUpdate{
					Versioning: &cmn.VersionConfToUpdate{
						Enabled: api.Bool(false),
					},
					Mirror: &cmn.MirrorConfToUpdate{
						Enabled: api.Bool(true),
						Copies:  api.Int64(120),
						Burst:   api.Int64(9560),
					},
					EC: &cmn.ECConfToUpdate{
						Enabled:      api.Bool(true),
						ParitySlices: api.Int(1024),
						ObjSizeLimit: api.Int64(0),
						Compression:  api.String(""),
					},
					Cksum: &cmn.CksumConfToUpdate{
						Type:            api.String(cmn.PropInherit),
						ValidateWarmGet: api.Bool(true),
					},
					AccessAttrs: api.Uint64(12),
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
				"provider": cmn.ProviderAIS,
			}),
			Entry("field not found", &Foo{}, map[string]interface{}{
				"foo.bar": 2,
			}),
		)
	})
})
