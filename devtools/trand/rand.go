// Package trand provides random string for devtools and tests
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package trand

import (
	"math/rand"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func String(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = cos.LetterRunes[rand.Int63()%int64(cos.LenRunes)]
	}
	return string(b)
}
