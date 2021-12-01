// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand"

	"github.com/NVIDIA/aistore/cmn/mono"
)

const letterRunes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type cryptoRandSource struct{}

var strongRandSource rand.Source = &cryptoRandSource{}

func (*cryptoRandSource) Int63() int64 {
	var buf [8]byte
	// crypto/rand uses syscalls or reads from /dev/random (or /dev/urandom) to get random bytes.
	// See https://golang.org/pkg/crypto/rand/#pkg-variables for more.
	_, err := cryptorand.Read(buf[:])
	AssertNoErr(err)
	return int64(binary.LittleEndian.Uint64(buf[:]))
}
func (*cryptoRandSource) Seed(int64) {}

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterRunes[rand.Int63()%int64(len(letterRunes))]
	}
	return string(b)
}

func RandStringWithSrc(src rand.Source, n int) string {
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterRunes) {
			b[i] = letterRunes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func RandStringStrong(n int) string {
	return RandStringWithSrc(strongRandSource, n)
}

func NowRand() *rand.Rand {
	return rand.New(rand.NewSource(mono.NanoTime()))
}

func GenDaemonID() string {
	return RandStringStrong(8)
}
