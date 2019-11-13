// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"reflect"
	"testing"
)

func TestSplitEntryName(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "a/b/c",
			want: []string{"a/", "b/", "c"},
		},
		{
			name: "a/b/c/",
			want: []string{"a/", "b/", "c/"},
		},
		{
			name: "abc/def/ghi/",
			want: []string{"abc/", "def/", "ghi/"},
		},
		{
			name: "a/",
			want: []string{"a/"},
		},
		{
			name: "a",
			want: []string{"a"},
		},
	}
	for _, tt := range tests {
		if got := splitEntryName(tt.name); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitEntryName() = %v, want %v", got, tt.want)
		}
	}
}
