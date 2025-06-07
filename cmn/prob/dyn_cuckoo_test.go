// Package prob implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package prob_test

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/tools/trand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	testFilterInitSize = 100 * 1000
	objNameLength      = 5
	numKeys            = 100_000 // Fixed input size for benchmarking; avoids scaling issues from using b.N as input size.
	smallFilterSize    = 10    // Used for testing dynamic growth
)

// Predefined buckets.
var buckets = []string{
	"test", "imagenet", "cifar", "secret", "something-t1-d1345",
}

func randObjName(n int) []byte {
	return []byte(buckets[rand.IntN(len(buckets))] + "/" + trand.String(n))
}

func genKeys(keysNum int) [][]byte {
	keys := make([][]byte, keysNum)
	for i := range keysNum {
		keys[i] = randObjName(objNameLength)
	}
	return keys
}

var _ = Describe("Filter", func() {
	filter := prob.NewFilter(testFilterInitSize)

	BeforeEach(func() {
		filter.Reset()
	})

	Context("Lookup", func() {
		It("should correctly lookup a key in filter", func() {
			key := []byte("key")
			filter.Insert(key)
			Expect(filter.Lookup(key)).To(BeTrue())
		})

		It("should lookup the keys in filter with no more than 0.06% failure rate", func() {
			keys := genKeys(testFilterInitSize * 10)
			total := float64(len(keys))
			for _, key := range keys {
				filter.Insert(key)
			}

			failures := 0
			for _, key := range keys {
				if !filter.Lookup(key) {
					failures++
				}
			}

			Expect(float64(failures) / total * 100).To(BeNumerically("<=", 0.006*total))
		})
	})

	Context("Delete", func() {
		It("should correctly delete a key from filter", func() {
			key := []byte("key")
			filter.Insert(key)
			Expect(filter.Lookup(key)).To(BeTrue())
			filter.Delete(key)
			Expect(filter.Lookup(key)).To(BeFalse())
			filter.Delete(key) // try to delete already deleted key
			Expect(filter.Lookup(key)).To(BeFalse())

			// do it again to check if the filter wasn't broken
			filter.Insert(key)
			Expect(filter.Lookup(key)).To(BeTrue())
			filter.Delete(key)
			Expect(filter.Lookup(key)).To(BeFalse())
		})
	})
})

func BenchmarkInsert(b *testing.B) {
	b.Run("preallocated", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(uint(numKeys))

		i := 0
		for b.Loop() {
			filter.Insert(keys[i%len(keys)])
			i++
		}
	})

	b.Run("empty", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(smallFilterSize)

		i := 0
		for b.Loop() {
			filter.Insert(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkLookup(b *testing.B) {
	b.Run("single filter", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(uint(numKeys))

		for _, k := range keys {
			filter.Insert(k)
		}

		i := 0
		for b.Loop() {
			filter.Lookup(keys[i%len(keys)])
			i++
		}
	})

	b.Run("multiple filters", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(smallFilterSize)

		for _, k := range keys {
			filter.Insert(k)
		}

		i := 0
		for b.Loop() {
			filter.Lookup(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	b.Run("single filter", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(uint(numKeys))

		for _, k := range keys {
			filter.Insert(k)
		}

		i := 0
		for b.Loop() {
			filter.Delete(keys[i%len(keys)])
			i++
		}
	})

	b.Run("multiple filters", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(10)

		for _, k := range keys {
			filter.Insert(k)
		}

		i := 0
		for b.Loop() {
			filter.Delete(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkInsertAndDeleteAndLookupParallel(b *testing.B) {
	// NOTE: We intentionally use range b.N instead of b.Loop() because the
	// b.Loop() is not concurrent safe and can lead to race conditions.
	b.Run("preallocated", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(uint(numKeys))

		b.ResetTimer()

		wg := &sync.WaitGroup{}
		wg.Add(3)

		go func() {
			for i := range b.N {
				filter.Insert(keys[i%len(keys)])
			}
			wg.Done()
		}()
		go func() {
			for i := range b.N {
				filter.Lookup(keys[i%len(keys)])
			}
			wg.Done()
		}()
		go func() {
			for i := range b.N {
				filter.Delete(keys[i%len(keys)])
			}
			wg.Done()
		}()
		wg.Wait()
	})

	b.Run("empty", func(b *testing.B) {
		keys := genKeys(numKeys)
		filter := prob.NewFilter(uint(smallFilterSize))

		b.ResetTimer()

		wg := &sync.WaitGroup{}
		wg.Add(3)

		go func() {
			for i := range b.N {
				filter.Insert(keys[i%len(keys)])
			}
			wg.Done()
		}()
		go func() {
			for i := range b.N {
				filter.Lookup(keys[i%len(keys)])
			}
			wg.Done()
		}()
		go func() {
			for i := range b.N {
				filter.Delete(keys[i%len(keys)])
			}
			wg.Done()
		}()
		wg.Wait()
	})
}
