// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"reflect"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	. "github.com/onsi/ginkgo/v2"
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
		barInline struct {
			Foo `json:",inline"`
			C   string `json:"c"`
		}
	)

	Describe("IterFields", func() {
		It("should handle omit tags correctly", func() {
			got := make(map[string]any)
			err := cmn.IterFields(Foo{A: 1, B: 2}, func(tag string, field cmn.IterField) (error, bool) {
				got[tag] = field.Value()
				return nil, false
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(map[string]any{"b": 2}))
		})

		It("should iterate nested structs correctly", func() {
			v := bar{Foo: Foo{A: 3, B: 10}, C: "string"}
			got := make(map[string]any)
			err := cmn.IterFields(v, func(tag string, field cmn.IterField) (error, bool) {
				got[tag] = field.Value()
				return nil, false
			}, cmn.IterOpts{VisitAll: true})

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(HaveKeyWithValue("foo.b", 10))
			Expect(got).To(HaveKeyWithValue("c", "string"))
			Expect(got).To(HaveKey("foo")) // the struct itself
		})

		It("should handle inline fields correctly", func() {
			v := barInline{Foo: Foo{A: 3, B: 10}, C: "string"}
			got := make(map[string]any)
			err := cmn.IterFields(v, func(tag string, field cmn.IterField) (error, bool) {
				got[tag] = field.Value()
				return nil, false
			}, cmn.IterOpts{VisitAll: true})

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(HaveKeyWithValue("b", 10))
			Expect(got).To(HaveKeyWithValue("c", "string"))
			Expect(got).To(HaveKey("")) // inline struct
		})

		It("should enumerate BucketProps fields without breaking on additions", func() {
			bprops := cmn.Bprops{
				Provider:   apc.AIS,
				BackendBck: cmn.Bck{Name: "test", Provider: apc.GCP},
				EC:         cmn.ECConf{Enabled: true, ParitySlices: 2},
				Cksum:      cmn.CksumConf{Type: cos.ChecksumOneXxh},
			}

			got := make(map[string]any)
			err := cmn.IterFields(bprops, func(tag string, field cmn.IterField) (error, bool) {
				got[tag] = field.Value()
				return nil, false
			})
			Expect(err).NotTo(HaveOccurred())

			// Test that core expected fields exist
			coreFields := map[string]any{
				"provider":             apc.AIS,
				"backend_bck.name":     "test",
				"backend_bck.provider": apc.GCP,
				"ec.enabled":           true,
				"ec.parity_slices":     2,
				"checksum.type":        cos.ChecksumOneXxh,
			}

			for field, expectedValue := range coreFields {
				Expect(got).To(HaveKeyWithValue(field, expectedValue),
					"field %s should exist with value %v", field, expectedValue)
			}

			// Ensure we got a reasonable number of fields (allowing for growth)
			Expect(len(got)).To(BeNumerically(">=", 40), "should have at least 40 fields")
			Expect(len(got)).To(BeNumerically("<=", 100), "should have at most 100 fields (sanity check)")
		})
	})

	Describe("UpdateFieldValue", func() {
		It("should update BucketProps via string values", func() {
			bprops := &cmn.Bprops{}

			updates := map[string]any{
				"mirror.enabled":   "true",
				"mirror.copies":    "3",
				"ec.enabled":       true,
				"ec.parity_slices": 1024,
				"checksum.type":    cos.ChecksumOneXxh,
				"access":           "12",
			}

			for field, value := range updates {
				err := cmn.UpdateFieldValue(bprops, field, value)
				Expect(err).NotTo(HaveOccurred(), "failed to update field %s", field)
			}

			// Verify updates worked
			Expect(bprops.Mirror.Enabled).To(BeTrue())
			Expect(bprops.Mirror.Copies).To(Equal(int64(3)))
			Expect(bprops.EC.Enabled).To(BeTrue())
			Expect(bprops.EC.ParitySlices).To(Equal(1024))
			Expect(bprops.Cksum.Type).To(Equal(cos.ChecksumOneXxh))
			Expect(bprops.Access).To(Equal(apc.AccessAttrs(12)))
		})

		It("should update BpropsToSet via pointers", func() {
			bpropsToSet := &cmn.BpropsToSet{}

			updates := map[string]any{
				"mirror.enabled": "true",
				"mirror.copies":  "5",
				"ec.enabled":     false,
				"checksum.type":  cos.ChecksumOneXxh,
			}

			for field, value := range updates {
				err := cmn.UpdateFieldValue(bpropsToSet, field, value)
				Expect(err).NotTo(HaveOccurred(), "failed to update field %s", field)
			}

			// Verify pointer fields were set correctly
			Expect(bpropsToSet.Mirror).NotTo(BeNil())
			Expect(bpropsToSet.Mirror.Enabled).To(Equal(apc.Ptr(true)))
			Expect(bpropsToSet.Mirror.Copies).To(Equal(apc.Ptr(int64(5))))
			Expect(bpropsToSet.EC).NotTo(BeNil())
			Expect(bpropsToSet.EC.Enabled).To(Equal(apc.Ptr(false)))
			Expect(bpropsToSet.Cksum).NotTo(BeNil())
			Expect(bpropsToSet.Cksum.Type).To(Equal(apc.Ptr(cos.ChecksumOneXxh)))
		})

		It("should handle type conversions correctly", func() {
			bprops := &cmn.Bprops{}

			// Test various type conversions that should work
			testCases := []struct {
				field  string
				value  any
				verify func()
			}{
				{
					field:  "mirror.burst_buffer",
					value:  "1024",
					verify: func() { Expect(bprops.Mirror.Burst).To(Equal(1024)) },
				},
				{
					field: "lru.dont_evict_time",
					value: "2h",
					verify: func() {
						expected, _ := time.ParseDuration("2h")
						actual := time.Duration(bprops.LRU.DontEvictTime)
						Expect(actual).To(Equal(expected))
					},
				},
				{
					field:  "access",
					value:  "255",
					verify: func() { Expect(bprops.Access).To(Equal(apc.AccessAttrs(255))) },
				},
			}

			for _, tc := range testCases {
				err := cmn.UpdateFieldValue(bprops, tc.field, tc.value)
				Expect(err).NotTo(HaveOccurred(), "failed to update field %s", tc.field)
				tc.verify()
			}
		})

		It("should error on invalid operations", func() {
			// Test error cases
			errorCases := []struct {
				desc   string
				target any
				field  string
				value  any
			}{
				{
					desc:   "non-pointer struct",
					target: cmn.Bprops{},
					field:  "mirror.enabled",
					value:  true,
				},
				{
					desc:   "readonly field",
					target: &cmn.Bprops{},
					field:  "provider",
					value:  apc.GCP,
				},
				{
					desc:   "non-existent field",
					target: &Foo{},
					field:  "non.existent",
					value:  "value",
				},
			}

			for _, tc := range errorCases {
				err := cmn.UpdateFieldValue(tc.target, tc.field, tc.value)
				Expect(err).To(HaveOccurred(), "case '%s' should have failed", tc.desc)
			}
		})
	})

	Describe("Round-trip consistency", func() {
		It("should maintain consistency between IterFields and UpdateFieldValue", func() {
			// Create a BucketProps with some values
			original := &cmn.Bprops{
				Provider: apc.AIS,
				Mirror:   cmn.MirrorConf{Enabled: true, Copies: 3},
				EC:       cmn.ECConf{Enabled: false, ParitySlices: 4},
				Cksum:    cmn.CksumConf{Type: cos.ChecksumOneXxh},
			}

			// Extract all field values
			fields := make(map[string]any)
			err := cmn.IterFields(*original, func(tag string, field cmn.IterField) (error, bool) {
				fields[tag] = field.Value()
				return nil, false
			})
			Expect(err).NotTo(HaveOccurred())

			// Create a new struct and apply the same values (skipping readonly fields)
			target := &cmn.Bprops{}
			readonlyFields := map[string]bool{
				"provider": true,
				"created":  true,
			}

			for fieldName, value := range fields {
				if readonlyFields[fieldName] {
					continue
				}

				// Convert to string for more realistic test (like config updates)
				var stringValue string
				switch v := value.(type) {
				case string:
					stringValue = v
				case bool:
					stringValue = strconv.FormatBool(v)
				case int, int64:
					stringValue = strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
				case cos.Duration:
					stringValue = v.String()
				default:
					// Skip complex types for this test
					continue
				}

				err := cmn.UpdateFieldValue(target, fieldName, stringValue)
				Expect(err).NotTo(HaveOccurred(), "failed to update field %s with value %s", fieldName, stringValue)
			}

			// Verify key fields match
			Expect(target.Mirror.Enabled).To(Equal(original.Mirror.Enabled))
			Expect(target.Mirror.Copies).To(Equal(original.Mirror.Copies))
			Expect(target.EC.Enabled).To(Equal(original.EC.Enabled))
			Expect(target.EC.ParitySlices).To(Equal(original.EC.ParitySlices))
			Expect(target.Cksum.Type).To(Equal(original.Cksum.Type))
		})
	})

	Describe("MergeConfigToSet", func() {
		It("should merge configurations correctly", func() {
			orig := &cmn.ConfigToSet{
				Mirror: &cmn.MirrorConfToSet{
					Enabled: apc.Ptr(true),
					Copies:  apc.Ptr[int64](2),
				},
			}

			merge := &cmn.ConfigToSet{
				Mirror: &cmn.MirrorConfToSet{
					Enabled: apc.Ptr(false), // Should override
				},
				EC: &cmn.ECConfToSet{
					Enabled: apc.Ptr(true), // Should add
				},
			}

			orig.Merge(merge)

			// Verify merge results
			Expect(orig.Mirror.Enabled).To(Equal(apc.Ptr(false)))   // overridden
			Expect(orig.Mirror.Copies).To(Equal(apc.Ptr[int64](2))) // preserved
			Expect(orig.EC).NotTo(BeNil())                          // added
			Expect(orig.EC.Enabled).To(Equal(apc.Ptr(true)))        // added
		})

		It("should handle nested backend configurations", func() {
			orig := &cmn.ConfigToSet{
				Backend: &cmn.BackendConf{
					Conf: map[string]any{
						apc.AWS: map[string]any{"key1": "value1"},
					},
				},
			}

			merge := &cmn.ConfigToSet{
				Backend: &cmn.BackendConf{
					Conf: map[string]any{
						apc.AWS: map[string]any{"key2": "value2"},
						apc.GCP: map[string]any{"gcp_key": "gcp_value"},
					},
				},
			}

			orig.Merge(merge)

			// Verify AWS section was completely replaced (not merged)
			awsConf := orig.Backend.Conf[apc.AWS].(map[string]any)
			Expect(awsConf).To(HaveKeyWithValue("key2", "value2"))
			Expect(awsConf).NotTo(HaveKey("key1")) // replaced, not merged

			// Verify GCP section was added
			gcpConf := orig.Backend.Conf[apc.GCP].(map[string]any)
			Expect(gcpConf).To(HaveKeyWithValue("gcp_key", "gcp_value"))
		})
	})
})
