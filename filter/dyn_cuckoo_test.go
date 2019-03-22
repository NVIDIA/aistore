// Package filter implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package filter

import (
	"math/rand"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testFilterInitSize = 100 * 1000
	objNameLength      = 5
)

var (
	// predefined buckets
	buckets = []string{
		"test", "imagenet", "cifar", "secret", "something-t1-d1345",
	}

	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randObjName(n int) []byte {
	obj := buckets[rand.Intn(len(buckets))] + "/"
	for i := 0; i < n; i++ {
		obj += string(letterRunes[rand.Intn(len(letterRunes))])
	}
	return []byte(obj)
}

func genKeys(keysNum int) [][]byte {
	keys := make([][]byte, keysNum)
	for i := 0; i < keysNum; i++ {
		keys[i] = randObjName(objNameLength)
	}
	return keys
}

var _ = Describe("Filter", func() {
	filter := NewFilter(testFilterInitSize)

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
		keys := genKeys(b.N)
		filter := NewFilter(uint(b.N))

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}
	})

	b.Run("empty", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(10)

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}
	})
}

func BenchmarkLookup(b *testing.B) {
	b.Run("single filter", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(uint(b.N))
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Lookup(keys[n])
		}
	})

	b.Run("multiple filters", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(10)
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Lookup(keys[n])
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	b.Run("single filter", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(uint(b.N))
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Delete(keys[n])
		}
	})

	b.Run("multiple filters", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(10)
		for n := 0; n < b.N; n++ {
			filter.Insert(keys[n])
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			filter.Delete(keys[n])
		}
	})
}

func BenchmarkInsertAndDeleteAndLookupParallel(b *testing.B) {
	b.Run("preallocated", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(uint(b.N))

		b.ResetTimer()

		wg := &sync.WaitGroup{}
		wg.Add(3)
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Insert(keys[n])
			}
			wg.Done()
		}()
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Lookup(keys[n])
			}
			wg.Done()
		}()
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Delete(keys[n])
			}
			wg.Done()
		}()
		wg.Wait()
	})

	b.Run("empty", func(b *testing.B) {
		keys := genKeys(b.N)
		filter := NewFilter(10)

		b.ResetTimer()

		wg := &sync.WaitGroup{}
		wg.Add(3)
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Insert(keys[n])
			}
			wg.Done()
		}()
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Lookup(keys[n])
			}
			wg.Done()
		}()
		go func() {
			for n := 0; n < b.N; n++ {
				filter.Delete(keys[n])
			}
			wg.Done()
		}()
		wg.Wait()
	})
}
