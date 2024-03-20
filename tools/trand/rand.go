// Package trand provides random string for dev tools and tests
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package trand

import (
	"math/rand"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func String(n int) string {
	b := make([]byte, n)
	for i := range n {
		b[i] = cos.LetterRunes[rand.Int63()%int64(cos.LenRunes)]
	}
	return string(b)
}
