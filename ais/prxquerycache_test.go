// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryCache+QueryBuffer", func() {
	makeEntries := func(xs ...string) (entries []*cmn.BucketEntry) {
		for _, x := range xs {
			entries = append(entries, &cmn.BucketEntry{
				Name: x,
			})
		}
		return
	}

	extractNames := func(entries []*cmn.BucketEntry) (xs []string) {
		for _, entry := range entries {
			xs = append(xs, entry.Name)
		}
		return
	}

	Describe("QueryCache", func() {
		var (
			id    = cacheReqID{bck: cmn.Bck{Name: "some_bck"}}
			cache *queryCaches
		)

		BeforeEach(func() {
			cache = &queryCaches{}
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
			cache.set(id, "f", []*cmn.BucketEntry{}, 4)

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
			var (
				otherID = cacheReqID{bck: cmn.Bck{Name: "something"}}
			)

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
	})

	Describe("QueryBuffer", func() {
		var (
			id     = "some_id"
			buffer *queryBuffers
		)

		BeforeEach(func() {
			buffer = &queryBuffers{}
		})

		It("should correctly create single buffer", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "g"), 3)
			buffer.set(id, "target2", makeEntries("b", "c", "h"), 3)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries := buffer.get(id, "", 6)
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c", "d", "e", "f"}))
			// Since `f` is the smallest of the last elements we cannot join:
			// `g` and `h` because there might be still some elements after `f`
			// in `target3`. Therefore, we should answer "not enough" to next calls.
			hasEnough := buffer.hasEnough(id, "f", 1)
			Expect(hasEnough).To(BeFalse())
		})

		It("should correctly append new entries", func() {
			buffer.set(id, "target1", makeEntries("a", "d", "g"), 3)
			buffer.set(id, "target2", makeEntries("b", "c", "h"), 3)
			buffer.set(id, "target3", makeEntries("e", "f"), 2)

			entries := buffer.get(id, "", 3)
			Expect(extractNames(entries)).To(Equal([]string{"a", "b", "c"}))
			hasEnough := buffer.hasEnough(id, "d", 4)
			Expect(hasEnough).To(BeFalse())

			last := buffer.last(id, "d")
			// `f` is the smallest of the last elements of all targets, so it
			// should be next continuation token.
			Expect(last).To(Equal("f"))

			// Now we simulate receiving entries after `f` token.
			buffer.set(id, "target1", makeEntries("g", "k", "m"), 3)
			buffer.set(id, "target2", makeEntries("h", "i", "n"), 3)
			buffer.set(id, "target3", makeEntries("j", "l"), 2)

			entries = buffer.get(id, "", 9)
			Expect(extractNames(entries)).To(Equal([]string{"d", "e", "f", "g", "h", "i", "j", "k", "l"}))
			hasEnough = buffer.hasEnough(id, "l", 1)
			Expect(hasEnough).To(BeFalse())
		})

		It("should correctly identify no objects", func() {
			hasEnough := buffer.hasEnough("id", "a", 10)
			Expect(hasEnough).To(BeFalse())
			hasEnough = buffer.hasEnough("id", "a", 10)
			Expect(hasEnough).To(BeFalse())
		})
	})
})
