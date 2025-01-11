// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// file name too long (0x24)
const (
	prefixFntl = ".x"

	// a typical local FS limitation
	maxLenBasename = 255

	// a.k.a. maximum FQN length before we decide to shorten it
	// given that Linux PATH_MAX = 4096 (see /usr/include/limits.h)
	// this number also takes into account:
	// - maxLenMountpath = 255 (see cmn/config)
	// - max bucket name = 64  (see cos.CheckAlphaPlus)
	maxLenPath = 3072
)

func HasPrefixFntl(s string) bool {
	return strings.HasPrefix(s, prefixFntl)
}

func ShortenFntl(s string) string {
	return prefixFntl + cos.ChecksumB2S(cos.UnsafeB(s), cos.ChecksumSHA256)
}

func IsFntl(objName string) bool {
	if l := len(objName); l > maxLenBasename {
		if l > maxLenPath || len(filepath.Base(objName)) > maxLenBasename {
			return true
		}
	}
	return false
}
