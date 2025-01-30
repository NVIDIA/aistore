// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import "errors"

func numOpenFiles() (int, error) {
	return 0, errors.New("num-open-files not implemented yet")
}
