// Package cryptorand
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cryptorand

import "crypto/rand"

func Read(buf []byte) (int, error) { return rand.Read(buf) }
