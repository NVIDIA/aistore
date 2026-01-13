// Package prob implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package prob_test

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/tools/trand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	objNameLength  = 5
	defaultNumKeys = 10_000_000 // default filter is 10M in size
	dynamicNumKeys = 5 * 10_000_000
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
	filter := prob.NewDefaultFilter()

	BeforeEach(func() {
		if testing.Short() {
			Skip("skipping prob_test in short mode")
		}
		filter.Reset()
	})

	Context("Lookup", func() {
		It("should correctly lookup a key in filter", func() {
			key := []byte("key")
			filter.Insert(key)
			Expect(filter.Lookup(key)).To(BeTrue())
		})

		It("should lookup the keys in filter with no more than 0.06% failure rate", func() {
			keys := genKeys(defaultNumKeys * 5)
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
	b.Run("default", func(b *testing.B) {
		keys := genKeys(defaultNumKeys)
		filter := prob.NewDefaultFilter()

		for i := 0; b.Loop(); i++ {
			filter.Insert(keys[i%len(keys)])
		}
	})

	b.Run("dynamic growth", func(b *testing.B) {
		keys := genKeys(dynamicNumKeys)
		filter := prob.NewDefaultFilter()

		for i := 0; b.Loop(); i++ {
			filter.Insert(keys[i%len(keys)])
		}
	})
}

func BenchmarkLookup(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		keys := genKeys(defaultNumKeys)
		filter := prob.NewDefaultFilter()

		for _, k := range keys {
			filter.Insert(k)
		}

		for i := 0; b.Loop(); i++ {
			filter.Lookup(keys[i%len(keys)])
		}
	})

	b.Run("dynamic growth", func(b *testing.B) {
		keys := genKeys(dynamicNumKeys)
		filter := prob.NewDefaultFilter()

		for _, k := range keys {
			filter.Insert(k)
		}

		for i := 0; b.Loop(); i++ {
			filter.Lookup(keys[i%len(keys)])
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		keys := genKeys(defaultNumKeys)
		filter := prob.NewDefaultFilter()

		for _, k := range keys {
			filter.Insert(k)
		}

		for i := 0; b.Loop(); i++ {
			filter.Delete(keys[i%len(keys)])
		}
	})

	b.Run("dynamic growth", func(b *testing.B) {
		keys := genKeys(dynamicNumKeys)
		filter := prob.NewDefaultFilter()

		for _, k := range keys {
			filter.Insert(k)
		}

		for i := 0; b.Loop(); i++ {
			filter.Delete(keys[i%len(keys)])
		}
	})
}

func BenchmarkConcurrentLoad(b *testing.B) {
	b.Run("default", func(b *testing.B) {
		// 10% of total keys inserted
		const insertRatio = 0.1

		// Match RunParallel's default GOMAXPROCS
		gfnSenders := runtime.NumCPU()

		keys := genKeys(defaultNumKeys)
		insertKeys := keys[:int(float64(defaultNumKeys)*insertRatio)]
		filter := prob.NewDefaultFilter()

		var (
			stopInsert atomic.Bool
			wg         sync.WaitGroup
		)

		// Insert GFN keys to simulate background work
		for i := range gfnSenders {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				start := id * len(insertKeys) / gfnSenders
				end := start + len(insertKeys)/gfnSenders
				for !stopInsert.Load() {
					for j := start; j < end; j++ {
						filter.Insert(insertKeys[j])
					}
				}
			}(i)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%len(keys)]
				if filter.Lookup(key) {
					filter.Delete(key)
				}
				i++
			}
		})
		b.StopTimer()

		stopInsert.Store(true)
		wg.Wait()
	})

	b.Run("dynamic growth", func(b *testing.B) {
		// 75% of total keys to force the filter to grow
		const insertRatio = 0.75
		gfnSenders := runtime.NumCPU()

		keys := genKeys(dynamicNumKeys)
		insertKeys := keys[:int(float64(dynamicNumKeys)*insertRatio)]
		filter := prob.NewDefaultFilter()

		var (
			stopInsert atomic.Bool
			wg         sync.WaitGroup
		)

		// Note: We prepopulate with 10M keys to ensure the filter is already at initial capacity,
		// forcing dynamic resizing during the benchmark window
		for i := range defaultNumKeys {
			filter.Insert(insertKeys[i])
		}

		for i := range gfnSenders {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				start := id * len(insertKeys) / gfnSenders
				end := start + len(insertKeys)/gfnSenders
				for !stopInsert.Load() {
					for j := start; j < end; j++ {
						filter.Insert(insertKeys[j])
					}
				}
			}(i)
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := keys[i%len(keys)]
				if filter.Lookup(key) {
					filter.Delete(key)
				}
				i++
			}
		})
		b.StopTimer()

		stopInsert.Store(true)
		wg.Wait()
	})
}
