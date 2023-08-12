// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"os"
	"strings"
)

// See also: cmn/archive/mime.go & ext/dsort/shard/record.go

// e.g.: "/aaa/bbb/ccc.tar.gz" => ".tar.gz", while "/aaa/bb.b/ccctargz"  => ""
func Ext(path string) (ext string) {
	for i := len(path) - 1; i >= 0 && !os.IsPathSeparator(path[i]); i-- {
		if path[i] == '.' {
			ext = path[i:]
		}
	}
	return
}

// WebDataset convention - not to confuse with filepath.Base (!)
// * see https://github.com/webdataset/webdataset#the-webdataset-format
func Basename(path string) string {
	return strings.TrimSuffix(path, Ext(path))
}
