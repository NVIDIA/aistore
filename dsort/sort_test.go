/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"fmt"

	"github.com/NVIDIA/dfcpub/dsort/extract"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func createRecords(keys ...interface{}) *extract.Records {
	records := extract.NewRecords(len(keys))
	for _, key := range keys {
		records.Insert(&extract.Record{Key: key, ContentPath: fmt.Sprintf("%v", key)})
	}
	return records
}

var _ = Describe("SortRecords", func() {
	It("should sort records alphanumerically ascending", func() {
		expected := createRecords("abc", "def")
		fm := createRecords("abc", "def")
		sortRecords(fm, &SortAlgorithm{Decreasing: false, FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically ascending when already sorted", func() {
		expected := createRecords("abc", "def")
		fm := createRecords("def", "abc")
		sortRecords(fm, &SortAlgorithm{Decreasing: false, FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically descending", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("abc", "def")
		sortRecords(fm, &SortAlgorithm{Decreasing: true, FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically descending when already sorted", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("def", "abc")
		sortRecords(fm, &SortAlgorithm{Decreasing: true, FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically ascending when keys are ints", func() {
		expected := createRecords(int64(10), int64(20))
		fm := createRecords(int64(20), int64(10))
		sortRecords(fm, &SortAlgorithm{Decreasing: false, FormatType: extract.FormatTypeInt})
		Expect(fm).To(Equal(expected))
	})

	It("should sort records alphanumerically ascending when keys are floats", func() {
		expected := createRecords(float64(10.20), float64(20.10))
		fm := createRecords(float64(20.10), float64(10.20))
		sortRecords(fm, &SortAlgorithm{Decreasing: false, FormatType: extract.FormatTypeFloat})
		Expect(fm).To(Equal(expected))
	})

	It("should not sort records when none algorithm specified", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("def", "abc")
		sortRecords(fm, &SortAlgorithm{Kind: SortKindNone, FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})

	It("should shuffle records reprudacibally when same seed specified", func() {
		expected := createRecords("def", "abc")
		fm := createRecords("abc", "def")
		sortRecords(fm, &SortAlgorithm{Kind: SortKindShuffle, Seed: "1010102", FormatType: extract.FormatTypeString})
		Expect(fm).To(Equal(expected))
	})
})
