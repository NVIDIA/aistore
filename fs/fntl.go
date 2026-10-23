// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// file name too long (0x24)
const (
	prefixFntl = ".x"

	// shortened name: prefix + hex(sha256)
	lenShortFntl = len(prefixFntl) + 2*sha256.Size

	// a typical local FS limitation
	maxLenBasename = 255

	// a.k.a. maximum FQN length before we decide to shorten it
	// given that Linux PATH_MAX = 4096 (see /usr/include/limits.h)
	// this number also takes into account:
	// - maxLenMountpath = 255 (see cmn/config)
	// - max bucket name = 64  (cos.MaxNameLength)
	maxLenPath = 3072
)

func HasPrefixFntl(s string) bool {
	return strings.HasPrefix(s, prefixFntl)
}

// match the exact shape of a shortened name (see ShortenFntl)
func IsShortFntl(s string) bool {
	return len(s) == lenShortFntl && HasPrefixFntl(s)
}

// return prefix + hex(sha256(s)) - one allocation
func ShortenFntl(s string) string {
	var (
		buf [lenShortFntl]byte
		sum = sha256.Sum256(cos.UnsafeB(s))
	)
	copy(buf[:], prefixFntl)
	hex.Encode(buf[len(prefixFntl):], sum[:])
	return string(buf[:])
}

func IsFntl(objName string) bool {
	if l := len(objName); l > maxLenBasename {
		if l > maxLenPath || len(filepath.Base(objName)) > maxLenBasename {
			return true
		}
	}
	return false
}
