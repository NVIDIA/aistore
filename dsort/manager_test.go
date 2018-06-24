/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"reflect"
	"testing"
)

func TestMergeSortedMeta(t *testing.T) {
	{
		m := &Manager{SortedRecords: make([]Record, 0)}
		expected := []Record{
			{Key: "abc", Name: "first"},
			{Key: "def", Name: "second"},
			{Key: "ghi", Name: "third"}}
		m.MergeSortedRecords(expected)

		if len(m.SortedRecords) != len(expected) {
			t.Errorf("expected len: %d, got len: %d", len(expected), len(m.SortedRecords))
		}
		for i, v := range m.SortedRecords {
			if !reflect.DeepEqual(v, expected[i]) {
				t.Errorf("Record at index %d not equal. Expected: %+v, got: %+v", i, expected[i], v)
			}
		}
	}
	{
		initial := []Record{{Key: "a"}, {Key: "c"}, {Key: "e"}}
		toMerge := []Record{{Key: "b"}, {Key: "d"}}
		expected := []Record{{Key: "a"}, {Key: "b"}, {Key: "c"}, {Key: "d"}, {Key: "e"}}
		m := &Manager{SortedRecords: initial}
		m.MergeSortedRecords(toMerge)

		if len(m.SortedRecords) != len(expected) {
			t.Errorf("expected len: %d, got len: %d", len(expected), len(m.SortedRecords))
		}
		for i, v := range m.SortedRecords {
			if !reflect.DeepEqual(v, expected[i]) {
				t.Errorf("Record at index %d not equal. Expected: %+v, got: %+v", i, expected[i], v)
			}
		}
	}
	{
		initial := []Record{{Key: "c"}, {Key: "a"}}
		toMerge := []Record{{Key: "e"}, {Key: "d"}, {Key: "b"}}
		expected := []Record{{Key: "e"}, {Key: "d"}, {Key: "c"}, {Key: "b"}, {Key: "a"}}
		m := &Manager{SortedRecords: initial, sortAlgo: sortAlgorithm{Decreasing: true}}
		m.MergeSortedRecords(toMerge)

		if len(m.SortedRecords) != len(expected) {
			t.Errorf("expected len: %d, got len: %d", len(expected), len(m.SortedRecords))
		}
		for i, v := range m.SortedRecords {
			if !reflect.DeepEqual(v, expected[i]) {
				t.Errorf("Record at index %d not equal. Expected: %+v, got: %+v", i, expected[i], v)
			}
		}
	}
}

func TestInit(t *testing.T) {
	{
		m := &Manager{}
		sr := RequestSpec{Extension: ".jpg"}
		err := m.Init(sr, nil, "http://localhost:8081")
		if err == nil {
			t.Error("expected non-nil error from passing in invalid extension, got nil error")
		}
	}
	{
		m := &Manager{}
		sr := RequestSpec{Extension: ".tar"}
		err := m.Init(sr, nil, "http://localhost:8081")
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}
		if _, ok := m.ExtractCreater.(*tarExtractCreater); !ok {
			t.Errorf("expected m.ExtractCreater to be type tarExtractCreater, got type %s",
				reflect.TypeOf(m.ExtractCreater))
		}
		if m.ExtractCreater.UsingCompression() {
			t.Error("ExtractCreater for extension .tar should not be using compression")
		}
	}
	{
		m := &Manager{}
		sr := RequestSpec{Extension: ".tgz"}
		err := m.Init(sr, nil, "http://localhost:8081")
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}
		if _, ok := m.ExtractCreater.(*tarExtractCreater); !ok {
			t.Errorf("expected m.ExtractCreater to be type tarExtractCreater, got type %s",
				reflect.TypeOf(m.ExtractCreater))
		}
		if !m.ExtractCreater.UsingCompression() {
			t.Error("ExtractCreater for extension .tgz should be using compression")
		}
	}
	{
		m := &Manager{}
		sr := RequestSpec{Extension: ".tar.gz"}
		err := m.Init(sr, nil, "http://localhost:8081")
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}
		if _, ok := m.ExtractCreater.(*tarExtractCreater); !ok {
			t.Errorf("expected m.ExtractCreater to be type tarExtractCreater, got type %s",
				reflect.TypeOf(m.ExtractCreater))
		}
		if !m.ExtractCreater.UsingCompression() {
			t.Error("ExtractCreater for extension .tar.gz should be using compression")
		}
	}
}
