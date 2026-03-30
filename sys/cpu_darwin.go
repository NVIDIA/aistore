// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
)

var errDarwin = errors.New("darwin: no sys/cpu and sys/mem support")

func (*cpu) setNum() error              { return errDarwin }
func (*cpu) get() (int64, int64, error) { return 0, 0, errDarwin }
