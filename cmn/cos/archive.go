// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "strings"

// supported archive types (file extensions)
const (
	ExtTar    = ".tar"
	ExtTgz    = ".tgz"
	ExtTarTgz = ".tar.gz"
	ExtZip    = ".zip"
)

var ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip}

func IsGzipped(filename string) bool {
	return strings.HasSuffix(filename, ExtTgz) || strings.HasSuffix(filename, ExtTarTgz)
}
