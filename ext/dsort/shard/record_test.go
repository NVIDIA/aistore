// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard_test

import (
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Records", func() {
	const objectSize = 100

	Context("insert", func() {
		It("should insert record", func() {
			records := shard.NewRecords(0)
			records.Insert(&shard.Record{
				Key:  "some_key",
				Name: "some_key",
				Objects: []*shard.RecordObj{
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})
			records.Insert(&shard.Record{
				Key:  "some_key1",
				Name: "some_key1",
				Objects: []*shard.RecordObj{
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})

			Expect(records.Len()).To(Equal(2))
		})

		It("should insert record but merge it", func() {
			records := shard.NewRecords(0)
			records.Insert(&shard.Record{
				Key:  "some_key",
				Name: "some_key",
				Objects: []*shard.RecordObj{
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".txt",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".jpg",
					},
				},
			})

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(3))
			r := records.All()[0]
			Expect(r.TotalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))

			records.Insert(&shard.Record{
				Key:  "some_key",
				Name: "some_key",
				Objects: []*shard.RecordObj{
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".xml",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".png",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".tar",
					},
				},
			})

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(6))
			r = records.All()[0]
			Expect(r.TotalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))
		})

		It("should delete record obj", func() {
			records := shard.NewRecords(0)
			records.Insert(&shard.Record{
				Key:  "some_key",
				Name: "some_key",
				Objects: []*shard.RecordObj{
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".txt",
					},
					{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".jpg",
					},
				},
			})

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(3))
			r := records.All()[0]
			Expect(r.TotalSize()).To(BeEquivalentTo(3 * objectSize))

			records.DeleteDup(r.Name, ".cls")

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(2))
			Expect(records.All()[0].TotalSize()).To(BeEquivalentTo(2 * objectSize))

			// Repeated deletion should be no-op.
			records.DeleteDup(r.Name, ".cls")

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(2))
			Expect(records.All()[0].TotalSize()).To(BeEquivalentTo(2 * objectSize))

			// But deletion of other record object should succeed.
			records.DeleteDup(r.Name, ".jpg")

			Expect(records.Len()).To(Equal(1))
			Expect(records.TotalObjectCount()).To(Equal(1))
			Expect(records.All()[0].TotalSize()).To(BeEquivalentTo(objectSize))
		})
	})
})
