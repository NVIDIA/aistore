/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func createRecords(keys ...string) *records {
	records := newRecords(len(keys))
	for _, key := range keys {
		records.insert(&record{Key: key, ContentPath: key})
	}
	return records
}

var _ = Describe("SortRecords", func() {
	It("should sort records alphanumerically ascending", func() {
		expected := createRecords("abc", "def")
		fm := createRecords("abc", "def")
		fm.Sort(&SortAlgorithm{Decreasing: false})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically ascending when already sorted", func() {
		expected := createRecords("abc", "def")
		fm := createRecords("def", "abc")
		fm.Sort(&SortAlgorithm{Decreasing: false})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically descending", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("abc", "def")
		fm.Sort(&SortAlgorithm{Decreasing: true})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically descending when already sorted", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("def", "abc")
		fm.Sort(&SortAlgorithm{Decreasing: true})
		Expect(fm).To(Equal(expected))
	})

	It("should not sort records when none algorithm specified", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("def", "abc")
		fm.Sort(&SortAlgorithm{Kind: SortKindNone})
		Expect(fm).To(Equal(expected))
	})

	It("should shuffle records reprudacibally when same seed specified", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("abc", "def")
		fm.Sort(&SortAlgorithm{Kind: SortKindShuffle, Seed: "1010102"})
		Expect(fm).To(Equal(expected))
	})
})
