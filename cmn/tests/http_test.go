// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestMatchRESTItems(t *testing.T) {
	tests := []struct {
		name string

		path       string
		itemsAfter int
		splitAfter bool
		items      []string

		expectedItems []string
		expectedErr   bool
	}{
		{
			name: "smoke",
			path: "/some/path/to/url", itemsAfter: 2, splitAfter: true, items: []string{"some", "path"},
			expectedItems: []string{"to", "url"},
		},
		{
			name: "minimal",
			path: "/some/path", itemsAfter: 0, splitAfter: false, items: []string{"some", "path"},
			expectedItems: []string{},
		},
		{
			name: "with_empty",
			path: "/some/path/", itemsAfter: 0, splitAfter: false, items: []string{"some", "path"},
			expectedItems: []string{},
		},
		{
			name: "dont_split_after",
			path: "/some/path/to/url", itemsAfter: 2, splitAfter: true, items: []string{"some", "path"},
			expectedItems: []string{"to", "url"},
		},
		{
			name: "more_items_after",
			path: "/some/path/to/url/more", itemsAfter: 2, splitAfter: true, items: []string{"some", "path"},
			expectedItems: []string{"to", "url", "more"},
		},
		{
			name: "more_items_after_without_split",
			path: "/some/path/to/url/more", itemsAfter: 2, splitAfter: false, items: []string{"some", "path"},
			expectedItems: []string{"to", "url/more"},
		},
		{
			name: "invalid_path",
			path: "/some/to/url/path", itemsAfter: 2, splitAfter: true, items: []string{"some", "path"},
			expectedErr: true,
		},
		{
			name: "too_short",
			path: "/some/to/url/path", itemsAfter: 3, splitAfter: false, items: []string{"some", "to"},
			expectedErr: true,
		},
		{
			name: "too_long",
			path: "/some/path/url", itemsAfter: 0, splitAfter: false, items: []string{"some", "path"},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		apiItems, err := cmn.ParseURL(test.path, test.items, test.itemsAfter, test.splitAfter)
		if err != nil && !test.expectedErr {
			t.Fatalf("test: %s, err: %v", test.name, err)
		} else if err == nil && test.expectedErr {
			t.Fatalf("test: %s, expected error", test.name)
		} else if err != nil && test.expectedErr {
			continue
		}
		if !reflect.DeepEqual(apiItems, test.expectedItems) {
			t.Fatalf("test: %s, items are not equal (got: %v, expected: %v)", test.name, apiItems, test.expectedItems)
		}
	}
}
