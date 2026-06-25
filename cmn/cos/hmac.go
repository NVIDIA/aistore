// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "crypto/hmac"

func CryptoEqual(a, b []byte) bool { return hmac.Equal(a, b) }
