// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ListObjectsCache+ListObjectsBuffer", func() {
	makeEntries := func(xs ...string) (entries []*cmn.ObjEntry) {
		for _, x := range xs {
			entries = append(entries, &cmn.ObjEntry{
				Name: x,
			})
		}
		return
	}

	extractNames := func(entries []*cmn.ObjEntry) (xs []string) {
		for _, entry := range entries {
			xs = append(xs, entry.Name)
		}
		return
	}

	Describe("ListObjectsCache", func() {
		var (
			id    = cacheReqID{bck: &cmn.Bck{Name: "some_bck"}}
			cache *lsobjCaches
		)

		BeforeEach(func() {
			cache = &lsobjCaches{}
		})

		It("should correctly add entries to cache", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			entries, hasEnough := cache.get(id, "", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c"}))
		})

		It("should correctly add streaming intervals", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "c", makeEntries("d", "e", "f"), 3)
			cache.set(id, "f", makeEntries("g", "h", "i"), 3)

			entries, hasEnough := cache.get(id, "", 9)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}))
		})

		It("should correctly handle last page", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "c", makeEntries("d", "e", "f"), 3)
			cache.set(id, "f", makeEntries("g", "h", "i"), 4)

			entries, hasEnough := cache.get(id, "", 10)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}))
		})

		It("should correctly handle empty last page", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "c", makeEntries("d", "e", "f"), 3)
			cache.set(id, "f", []*cmn.ObjEntry{}, 4)

			entries, hasEnough := cache.get(id, "", 10)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))
		})

		It("should correctly handle overlapping entries", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "a", makeEntries("d", "e", "f"), 3)

			entries, hasEnough := cache.get(id, "", 4)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "d", "e", "f"}))
		})

		It("should correctly merge intervals", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "g", makeEntries("h", "i", "j"), 3)

			_, hasEnough := cache.get(id, "", 3)
			Expect(hasEnough).To(BeTrue())
			_, hasEnough = cache.get(id, "", 4)
			Expect(hasEnough).To(BeFalse())

			_, hasEnough = cache.get(id, "g", 2)
			Expect(hasEnough).To(BeTrue())

			// Add interval in the middle.
			cache.set(id, "c", makeEntries("d", "e", "f", "g"), 4)

			// Check that now intervals are connected.
			entries, hasEnough := cache.get(id, "", 4)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d"}))
			entries, hasEnough = cache.get(id, "", 10)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}))
		})

		It("should correctly merge overlapping intervals", func() {
			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			cache.set(id, "g", makeEntries("h", "i", "j"), 3)
			cache.set(id, "a", makeEntries("b", "c", "d", "e", "f", "g", "h", "i"), 8)

			entries, hasEnough := cache.get(id, "", 10)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}))
		})

		Describe("prepend", func() {
			It("should correctly prepend interval", func() {
				cache.set(id, "c", makeEntries("d", "e", "f"), 3)
				cache.set(id, "", makeEntries("a", "b", "c"), 3)

				entries, hasEnough := cache.get(id, "", 6)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))
			})

			It("should correctly prepend overlapping intervals", func() {
				cache.set(id, "c", makeEntries("d", "e", "f"), 3)
				cache.set(id, "", makeEntries("a", "b", "c", "d", "e"), 5)

				entries, hasEnough := cache.get(id, "", 6)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))
			})
		})

		It("should discard interval if already exists", func() {
			cache.set(id, "", makeEntries("a", "b", "c", "d", "e"), 5)
			cache.set(id, "a", makeEntries("b", "c", "d"), 3)

			entries, hasEnough := cache.get(id, "", 5)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e"}))
		})

		It("should discard interval if already exists", func() {
			cache.set(id, "", makeEntries("a", "b", "c", "d", "e"), 5)
			cache.set(id, "", makeEntries("a", "b"), 2)

			entries, hasEnough := cache.get(id, "", 5)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e"}))
		})

		It("should return empty response if cache is empty", func() {
			entries, hasEnough := cache.get(id, "", 0)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())

			entries, hasEnough = cache.get(id, "", 1)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())

			entries, hasEnough = cache.get(id, "a", 1)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())
		})

		It("should correctly distinguish between different caches", func() {
			otherID := cacheReqID{bck: &cmn.Bck{Name: "something"}}

			cache.set(id, "", makeEntries("a", "b", "c"), 3)
			entries, hasEnough := cache.get(id, "", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c"}))

			// Check if `otherID` cache is empty.
			entries, hasEnough = cache.get(otherID, "", 3)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())

			cache.set(otherID, "", makeEntries("d", "e", "f"), 3)
			entries, hasEnough = cache.get(otherID, "", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"d", "e", "f"}))
		})

		Describe("prefix", func() {
			It("should get prefixed entries from `id='bck'` cache", func() {
				prefixID := cacheReqID{bck: id.bck, prefix: "p-"}

				cache.set(id, "", makeEntries("a", "p-b", "p-c", "p-d", "z"), 5)
				entries, hasEnough := cache.get(id, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"a", "p-b", "p-c"}))

				// Now check that getting for prefix works.
				entries, hasEnough = cache.get(prefixID, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-b", "p-c", "p-d"}))

				entries, hasEnough = cache.get(prefixID, "", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-b", "p-c"}))

				// User requests more than we have but also all the prefixes are
				// fully contained in the interval so we are sure that there isn't
				// more of them. Therefore, we should return what we have.
				entries, hasEnough = cache.get(prefixID, "", 4)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-b", "p-c", "p-d"}))
			})

			It("should get prefixed entries from `id='bck' cache (boundaries)", func() {
				cache.set(id, "", makeEntries("b", "d", "y"), 3)
				entries, hasEnough := cache.get(id, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"b", "d", "y"}))

				// Get entries with prefix `y`.
				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "y"}, "", 1)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"y"}))

				_, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "y"}, "", 2)
				Expect(hasEnough).To(BeFalse())

				// Get entries with prefix `b`.
				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "b"}, "", 1)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"b"}))

				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "b"}, "", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"b"}))

				// Get entries with prefix `a`.
				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "a"}, "", 1)
				Expect(hasEnough).To(BeTrue())
				Expect(entries).To(Equal([]*cmn.ObjEntry{}))

				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "a"}, "", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(entries).To(Equal([]*cmn.ObjEntry{}))

				// Make interval "last".
				cache.set(id, "y", makeEntries(), 1)

				// Get entries with prefix `y`.
				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "y"}, "", 1)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"y"}))

				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "y"}, "", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"y"}))

				// Get entries with prefix `ya`.
				entries, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "ya"}, "", 1)
				Expect(hasEnough).To(BeTrue())
				Expect(entries).To(Equal([]*cmn.ObjEntry{}))
			})

			It("should correctly behave in `id='bck'` cache if prefix is contained in interval but there aren't matching entries", func() {
				prefixID := cacheReqID{bck: id.bck, prefix: "b-"}

				cache.set(id, "", makeEntries("a", "p-b", "p-c", "p-d", "z"), 5)
				entries, hasEnough := cache.get(id, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"a", "p-b", "p-c"}))

				// It should correctly return no entries when prefixes are
				// contained in the interval but there is no such entries.
				entries, hasEnough = cache.get(prefixID, "", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(entries).To(Equal([]*cmn.ObjEntry{}))

				entries, hasEnough = cache.get(prefixID, "a", 2)
				Expect(hasEnough).To(BeTrue())
				Expect(entries).To(Equal([]*cmn.ObjEntry{}))
			})

			It("should correctly behave in `id='bck'` cache if prefix is out of the interval", func() {
				cache.set(id, "", makeEntries("b", "m", "p", "y"), 4)
				entries, hasEnough := cache.get(id, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"b", "m", "p"}))

				_, hasEnough = cache.get(cacheReqID{bck: id.bck, prefix: "z"}, "", 1)
				Expect(hasEnough).To(BeFalse())
			})

			It("should get prefixed entries from `id='bck+prefix'` cache", func() {
				prefixID := cacheReqID{bck: id.bck, prefix: "p-"}

				cache.set(id, "", makeEntries("p-a", "p-b", "p-c", "p-d", "p-e"), 5)
				entries, hasEnough := cache.get(prefixID, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-a", "p-b", "p-c"}))

				// Now check that getting for prefix works.
				entries, hasEnough = cache.get(prefixID, "p-b", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-c", "p-d", "p-e"}))

				_, hasEnough = cache.get(prefixID, "p-b", 4)
				Expect(hasEnough).To(BeFalse())
			})

			It("should fallback to `id='bck'` cache when there is not enough entries in `id='bck+prefix'` cache", func() {
				prefixID := cacheReqID{bck: id.bck, prefix: "p-"}

				cache.set(id, "", makeEntries("a", "p-b", "p-c", "p-d"), 4)
				cache.set(prefixID, "", makeEntries("p-b", "p-c"), 2)

				entries, hasEnough := cache.get(prefixID, "", 3)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-b", "p-c", "p-d"}))

				// Insert more into `id="bck+prefix"` cache end check that we can get from it.
				cache.set(prefixID, "p-c", makeEntries("p-d", "p-f", "p-g"), 3)
				entries, hasEnough = cache.get(prefixID, "", 5)
				Expect(hasEnough).To(BeTrue())
				Expect(extractNames(entries)).To(Equal([]string{"p-b", "p-c", "p-d", "p-f", "p-g"}))
			})
		})
	})

	Describe("ListObjectsBuffer", func() {
		var (
			id     = "some_id"
			buffer *lsobjBuffers
		)

		BeforeEach(func() {
			buffer = &lsobjBuffers{}
		})

		It("should correctly create single buffer", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "g"), 3)
			buffer.set(id, "target2", makeEntries("b", "c", "h"), 3)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries, hasEnough := buffer.get(id, "", 6)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))

			// Since `f` is the smallest of the last elements we cannot join:
			// `g` and `h` because there might be still some elements after `f`
			// in `target3`. Therefore, we should answer "not enough" to next calls.
			entries, hasEnough = buffer.get(id, "f", 1)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())
		})

		It("should correctly append new entries", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "g"), 3)
			buffer.set(id, "target2", makeEntries("b", "c", "h"), 3)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries, hasEnough := buffer.get(id, "", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c"}))
			entries, hasEnough = buffer.get(id, "c", 4)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())

			last := buffer.last(id, "c")
			// `f` is the smallest of the last elements of all targets, so it
			// should be next continuation token.
			Expect(last).To(Equal("f"))

			// Now we simulate receiving entries after `f` token.
			buffer.set(id, "target1", makeEntries("g", "k", "m"), 3)
			buffer.set(id, "target2", makeEntries("h", "i", "n"), 3)
			buffer.set(id, "target3", makeEntries("j", "l"), 2)

			entries, hasEnough = buffer.get(id, "c", 9)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"d", "e", "f", "g", "h", "i", "j", "k", "l"}))
			entries, hasEnough = buffer.get(id, "l", 1)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())
		})

		It("should correctly identify no objects", func() {
			entries, hasEnough := buffer.get("id", "a", 10)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())
			entries, hasEnough = buffer.get("id", "a", 10)
			Expect(hasEnough).To(BeFalse())
			Expect(entries).To(BeNil())
		})

		It("should correctly handle end of entries", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "e"), 4)
			buffer.set(id, "target2", makeEntries("b", "c", "f"), 4)

			entries, hasEnough := buffer.get(id, "", 7)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))

			entries, hasEnough = buffer.get(id, "f", 1)
			Expect(hasEnough).To(BeTrue())
			Expect(entries).To(HaveLen(0))
		})

		It("should correctly handle getting 0 entries", func() {
			buffer.set(id, "target1", makeEntries(), 2)
			buffer.set(id, "target2", makeEntries(), 2)

			entries, hasEnough := buffer.get(id, "", 7)
			Expect(hasEnough).To(BeTrue())
			Expect(entries).To(HaveLen(0))

			entries, hasEnough = buffer.get(id, "f", 1)
			Expect(hasEnough).To(BeTrue())
			Expect(entries).To(HaveLen(0))
		})

		It("should correctly handle rerequesting the page", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "g"), 3)
			buffer.set(id, "target2", makeEntries("b", "c", "h"), 3)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries, hasEnough := buffer.get(id, "", 2)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"a", "b"}))
			entries, hasEnough = buffer.get(id, "b", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"c", "d", "e"}))

			// Rerequest the page with token `b`.
			_, hasEnough = buffer.get(id, "b", 3)
			Expect(hasEnough).To(BeFalse())

			// Simulate targets resending data.
			buffer.set(id, "target1", makeEntries("d", "g"), 2)
			buffer.set(id, "target2", makeEntries("c", "h"), 2)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries, hasEnough = buffer.get(id, "b", 3)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"c", "d", "e"}))
			entries, hasEnough = buffer.get(id, "e", 1)
			Expect(hasEnough).To(BeTrue())
			Expect(extractNames(entries)).To(Equal([]string{"f"}))
			_, hasEnough = buffer.get(id, "f", 1)
			Expect(hasEnough).To(BeFalse())
		})
	})
})
