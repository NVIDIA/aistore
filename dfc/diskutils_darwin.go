// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import "errors"

// iostat -cdxtm 10
func (r *iostatrunner) run() (err error) {
	assert(false, "niy")
	return nil
}

func (r *iostatrunner) stop(err error) {
	assert(false, "niy")
}

func (r *iostatrunner) isZeroUtil(dev string) bool {
	return true
}

func (r *iostatrunner) getMaxUtil() (maxutil float64) {
	return float64(-1)
}

func CheckIostatVersion() error {
	return errors.New("Not yet implemented")
}
