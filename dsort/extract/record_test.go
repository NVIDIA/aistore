/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Records", func() {
	const objectSize = 100

	Context("insert", func() {
		It("should insert record", func() {
			records := NewRecords(0)
			records.Insert(&Record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*RecordObj{
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})
			records.Insert(&Record{
				Key:         "some_key1",
				ContentPath: "some_key1",
				Objects: []*RecordObj{
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})

			Expect(records.Len()).To(Equal(2))
		})

		It("should insert record but merge it", func() {
			records := NewRecords(0)
			records.Insert(&Record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*RecordObj{
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".txt",
					},
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".jpg",
					},
				},
			})

			Expect(records.Len()).To(Equal(1))
			Expect(records.objectCount()).To(Equal(3))
			r := records.All()[0]
			Expect(r.TotalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))

			records.Insert(&Record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*RecordObj{
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".xml",
					},
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".png",
					},
					&RecordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".tar",
					},
				},
			})

			Expect(records.Len()).To(Equal(1))
			Expect(records.objectCount()).To(Equal(6))
			r = records.All()[0]
			Expect(r.TotalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))
		})
	})
})
