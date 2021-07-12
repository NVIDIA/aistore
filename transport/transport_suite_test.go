// Package transport provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package transport_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
)

func TestTransport(*testing.T) {
	go hk.DefaultHK.Run()
}
