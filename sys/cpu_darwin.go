// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
)

var errNoTracker = errors.New("darwin: no cpu tracker")

func contDetected() bool { return false }

func (*cpu) setNum() error              { return errNoTracker }
func (*cpu) get() (int64, int64, error) { return 0, 0, errNoTracker }
