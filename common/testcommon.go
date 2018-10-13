/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package common provides common low-level types and utilities for all dfcpub projects
package common

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"testing"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func CheckFatal(err error, t *testing.T) {
	if err != nil {
		Tlogf("FATAL: %v\n", err)
		debug.PrintStack()
		t.Fatalf("FATAL: %v", err)
	}
}

func Tlogf(msg string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, msg, args...)
	}
}

func Tlogln(msg string) {
	Tlogf(msg + "\n")
}

func FastRandomFilename(src *rand.Rand, fnlen int) string {
	b := make([]byte, fnlen)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := fnlen-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}
