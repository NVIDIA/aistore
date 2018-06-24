/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSortRecords(t *testing.T) {
	{
		expected := []Record{{Key: "abc"}, {Key: "def"}}
		fm := []Record{{Key: "abc"}, {Key: "def"}}
		SortRecords(&fm, false)
		if !reflect.DeepEqual(fm, expected) {
			fmt.Errorf("expected: %+v, actual: %+v", expected, fm)
		}
	}
	{
		expected := []Record{{Key: "abc"}, {Key: "def"}}
		fm := []Record{{Key: "def"}, {Key: "abc"}}
		SortRecords(&fm, false)
		if !reflect.DeepEqual(fm, expected) {
			fmt.Errorf("expected: %+v, actual: %+v", expected, fm)
		}
	}
	{
		expected := []Record{{Key: "def"}, {Key: "abc"}}
		fm := []Record{{Key: "def"}, {Key: "abc"}}
		SortRecords(&fm, true)
		if !reflect.DeepEqual(fm, expected) {
			fmt.Errorf("expected: %+v, actual: %+v", expected, fm)
		}
	}
	{
		expected := []Record{{Key: "def"}, {Key: "abc"}}
		fm := []Record{{Key: "abc"}, {Key: "def"}}
		SortRecords(&fm, true)
		if !reflect.DeepEqual(fm, expected) {
			fmt.Errorf("expected: %+v, actual: %+v", expected, fm)
		}
	}
}
