// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

const (
	LetterRunes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	LenRunes    = len(LetterRunes)

	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

///////////
// crand //
///////////

type (
	// cryptorand
	crand struct{}
)

var crnd rand.Source = &crand{}

func (*crand) Uint64() uint64 {
	var buf [8]byte
	// crypto/rand uses syscalls or reads from /dev/random (or /dev/urandom) to get random bytes.
	// See https://golang.org/pkg/crypto/rand/#pkg-variables for more.
	_, err := cryptorand.Read(buf[:])
	debug.AssertNoErr(err)
	return binary.LittleEndian.Uint64(buf[:])
}

func (*crand) Seed(int64) {}

func CryptoRandS(n int) string { return RandStringWithSrc(crnd, n) }

//
// misc. rand utils
//

func RandStringWithSrc(src rand.Source, n int) string {
	b := make([]byte, n)
	// src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, src.Uint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Uint64(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < LenRunes {
			b[i] = LetterRunes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

//
// rand/v2 convenience + uniform usage
//

func NewRandSource(seed uint64) rand.Source {
	return rand.NewPCG(seed, seed)
}

func NewNowSource() rand.Source {
	seed := mono.NanoTime()
	return rand.NewPCG(uint64(seed), uint64(seed))
}

func NowRand() *rand.Rand {
	return rand.New(NewNowSource())
}
